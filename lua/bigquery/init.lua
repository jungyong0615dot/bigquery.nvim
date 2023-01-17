
local M = {}

local defaults = require'bigquery.defaults'

local Job = require("plenary.job")
local Path = require("plenary.path")

local curl = require("custom_curl")

local actions = require("telescope.actions")
local actions_state = require("telescope.actions.state")
local finders = require("telescope.finders")
local pickers = require("telescope.pickers")
local previewers = require("telescope.previewers")
local conf = require("telescope.config").values
local ts_utils = require("telescope.utils")
local defaulter = ts_utils.make_default_callable
local tprint = require("tprint")
local notify = require("notify")

function table.append(t1, t2)
	for i = 1, #t2 do
		t1[#t1 + 1] = t2[i]
	end
	return t1
end

local function timestamp_to_human(timestamp)
  if timestamp == nil then
    return nil
  else
    return os.date("%Y-%m-%d %H:%M:%S", timestamp / 1000)
  end
end

local function parse_schema(schema, parent)
	local schema_parsed = {}
	for _, field in ipairs(schema) do
		if parent ~= nil then
			colname = parent .. "." .. field.name
		else
			colname = field.name
		end

		if field.fields ~= nil then
			table.append(schema_parsed, parse_schema(field.fields, colname))
		else
			table.insert(schema_parsed, colname)
		end
	end
	return schema_parsed
end

local function open_floating()
	local width = vim.api.nvim_get_option("columns")
	local height = vim.api.nvim_get_option("lines")
	local win_height = math.ceil(height * 0.7 - 4)
	local win_width = math.ceil(width * 0.7)
	local row = math.ceil((height - win_height) / 2 - 1)
	local col = math.ceil((width - win_width) / 2)
	local buf = vim.api.nvim_create_buf(true, true)

	vim.api.nvim_buf_set_option(buf, "filetype", "markdown")
	vim.b[buf].parent_buf = vim.api.nvim_get_current_buf()
	local _ = vim.api.nvim_open_win(buf, true, {
		style = "minimal",
		relative = "editor",
		row = row,
		col = col,
		width = win_width,
		height = win_height,
		border = "rounded",
	})
	vim.w.is_floating_scratch = true
	return buf
end

local function total_fields(t_tbl)
	local fields = {}
	if t_tbl["fields"] == nil then
		return { t_tbl["name"] }
	else
		for _, field in pairs(t_tbl["fields"]) do
			local sub_fields = total_fields(field)
			for _, sub_field in pairs(sub_fields) do
				table.insert(fields, t_tbl["name"] .. "." .. sub_field)
			end
		end
	end
	return fields
end

local function flatten_row(json_row, schema)
	local flattened = {}
	for idx, val in ipairs(json_row) do
		if type(val["v"]) == "table" and val["v"]["f"] ~= nil then
			table.append(flattened, flatten_row(val.v.f, schema[idx]["fields"]))
		elseif type(val["v"]) == "table" and val["v"]["v"] == nil and val["v"]["f"] == nil then
			if schema[idx]["fields"] == nil then
				cand_flattens = {}
				for _, cands in ipairs(val.v) do
					table.insert(cand_flattens, cands.v)
				end

				table.insert(flattened, cand_flattens)
			elseif #val["v"] == 0 then
				for _ = 1, #total_fields(schema[idx]) do
					table.insert(flattened, "-")
				end
			else
				local cand_flattens = {}
				for _, cand in ipairs(val.v) do
					table.insert(cand_flattens, flatten_row(cand.v.f, schema[idx]["fields"]))
				end

				for i1, _ in ipairs(cand_flattens[1]) do
					tmp_flatten = {}
					for _, cands in ipairs(cand_flattens) do
						table.insert(tmp_flatten, cands[i1])
					end
					table.insert(flattened, tmp_flatten)
				end
			end
		elseif type(val["v"]) == "table" and val["v"]["f"] == nil and schema[idx].fields == nil then
			table.insert(flattened, flatten_row(val.v, nil))
		elseif type(val["v"]) == "table" and val["v"]["f"] == nil and schema[idx].fields ~= nil then
			table.insert(flattened, flatten_row(val.v, schema[idx].fields))
		elseif type(val["v"]) == "table" and #val["v"] == 0 then
			for _ = 1, #total_fields(schema[idx]) do
				table.insert(flattened, "-")
			end
		elseif type(val["v"]) == "userdata" then
			if schema == nil then
				table.insert(flattened, "-")
			elseif schema[idx] == nil then
				table.insert(flattened, "-")
			elseif schema[idx]["fields"] ~= nil then
				for _ = 1, #(total_fields(schema[idx])) do
					table.insert(flattened, "-")
				end
			else
				table.insert(flattened, "-")
			end
		else
			table.insert(flattened, val.v)
		end
	end
	return flattened
end

M.show_schema = function(bq_full_table, sampling_ratio)
	table_info = vim.split(bq_full_table, "%.")

	local bq_project = table_info[1]
	local bq_dataset = table_info[2]
	local bq_table = table_info[3]

	local url_info = "https://bigquery.googleapis.com/bigquery/v2/projects/"
		.. bq_project
		.. "/datasets/"
		.. bq_dataset
		.. "/tables/"
		.. bq_table

	local access_token
	access_toeken_job = Job:new({
		command = "gcloud",
		args = { "auth", "application-default", "print-access-token" },
		cwd = "/usr/bin",
		on_exit = function(j, return_val)
			access_token = j:result()[1]
		end,
	})
	access_toeken_job:after(function()
		curl.get(url_info, {
			headers = {
				Authorization = "Bearer " .. access_token,
			},
			callback = vim.schedule_wrap(function(out)
				local floating_buf = open_floating()
        local full_info_lines = {}
				local bqtable = vim.json.decode(out.body)
				local parsed_data = {
					{ "numRows", bqtable.numRows },
					{ "clustering", vim.json.encode(bqtable.clustering) },
					{ "timePartitioning", vim.json.encode(bqtable.timePartitioning) },
					{ "lastModifiedTime", timestamp_to_human(bqtable.lastModifiedTime) },
					{ "creationTime", timestamp_to_human(bqtable.creationTime) },
					{ "columns", parse_schema(bqtable.schema.fields, nil) },
				}

				local pretty_parsed_lines = {}
				for _, v in ipairs(parsed_data) do
					if type(v[2]) == "table" then
						table.insert(pretty_parsed_lines, v[1] .. ": ")
						for k2, v2 in ipairs(v[2]) do
							table.insert(pretty_parsed_lines, "- " .. k2 .. ": " .. v2)
						end
					else
						table.insert(pretty_parsed_lines, v[1] .. ": " .. v[2])
					end
				end
				pretty_parsed_lines[#pretty_parsed_lines + 1] = ""
				pretty_parsed_lines[#pretty_parsed_lines + 1] = "```sql"


				if bqtable.timePartitioning ~= nil then
					if bqtable.timePartitioning ~= nil then
						required_filter = string.format(
							"AND DATE(%s) = '%s'",
							bqtable.timePartitioning.field,
							os.date("%Y-%m-%d", os.time() - 60 * 60 * 24 * 2)
						)
					end
				else
					required_filter = ""
				end

				my_query = string.format(
					[[
          SELECT
            *
          FROM `%s.%s.%s` TABLESAMPLE SYSTEM (%s PERCENT)
          WHERE 
          1=1
          %s
          LIMIT 3
        ]],
					bq_project,
					bq_dataset,
					bq_table,
					sampling_ratio,
					required_filter
				)
        table.append(pretty_parsed_lines, vim.split(my_query, '\n'))
				pretty_parsed_lines[#pretty_parsed_lines + 1] = "````"
				pretty_parsed_lines[#pretty_parsed_lines + 1] = ""

				vim.api.nvim_buf_set_lines(floating_buf, 0, -1, false, pretty_parsed_lines)
        table.append(full_info_lines, pretty_parsed_lines)


				local post_body = vim.json.encode({
					query = my_query,
					maxResults = 3,
					useLegacySql = false,
					formatOptions = {
						useInt64Timestamp = false,
					},
				})

				curl.post("https://bigquery.googleapis.com/bigquery/v2/projects/" .. bq_project .. "/queries", {
					headers = {
						Authorization = "Bearer " .. access_token,
						["Content-Type"] = "application/json",
						["Content-Length"] = post_body:len(),
					},
					body = post_body,
					callback = vim.schedule_wrap(function(post_out)
						local resp_body = vim.json.decode(post_out.body)
						local flattened_rows = {}

            if resp_body.rows == nil then
              print("EMPTY ROWS!")
              return
            end

						for idx, _ in ipairs(resp_body.rows) do
							table.insert(flattened_rows, flatten_row(resp_body.rows[idx].f, resp_body.schema.fields))
						end
						local flattend_cols = parse_schema(resp_body.schema.fields, nil)

						local dumped = {}

						for col_idx, col in ipairs(flattend_cols) do
							flattend_row = { name = col }
							for row_idx, row in ipairs(flattened_rows) do
								if type(row[col_idx]) == "table" then
									-- flattend_row[col] = vim.json.encode(row[col_idx])
									if #row[col_idx] > 2 then
										encoded = vim.json.encode({ row[col_idx][1], row[col_idx][2] }) .. "..."
									else
										encoded = vim.json.encode(row[col_idx])
									end
									flattend_row[row_idx] = encoded
								else
									flattend_row[row_idx] = row[col_idx]
								end
							end
							table.insert(dumped, flattend_row)
						end

						table_header = { "name" }
						for idx = 1, #flattened_rows do
							table.insert(table_header, idx)
						end

						bq_pretty_table = tostring(
							tprint(dumped, { column = table_header, frame = tprint.FRAME_DOUBLE, widthDefault = 60 })
						)
						nlines = vim.api.nvim_buf_line_count(floating_buf)
						vim.api.nvim_buf_set_lines(
							floating_buf,
							nlines + 3,
							-1,
							false,
							vim.split(bq_pretty_table, "\n")
						)

						bq_pretty_table_full = tostring(
							tprint(dumped, { column = table_header, frame = tprint.FRAME_DOUBLE})
						)
            table.append(full_info_lines, vim.split(bq_pretty_table_full, "\n"))


          -- save as markdown file
          local bqpath = Path:new(M.dir_bqtable)
          local path_dataset = (bqpath / bq_project / bq_dataset)
          pcall(path_dataset.mkdir, path_dataset, { parents = true })
          local saving_lines = vim.api.nvim_buf_get_lines(floating_buf, 0, -1, false)


          vim.fn.writefile(saving_lines, tostring(path_dataset / (bq_table .. ".md")), "b")
          vim.fn.writefile(full_info_lines, tostring(path_dataset / (bq_table .. "_FULL.md")), "b")

					end),
				})
			end),
		})
	end)
	access_toeken_job:start()
end


M.ask_routine = function()

	local bq_full_table = ""

	vim.ui.input({ prompt = "Routine - Enter bigquery full path name: " }, function(input)
		bq_full_table = input
	end)

  M.edit_routines(bq_full_table)
end

local function arg_build(arguments)
  local arg_text = {}
  -- mprint(arguments)
  if arguments.dataType == nil and arguments.typeKind == nil and arguments.type == nil then
    for _, v in ipairs(arguments) do
      -- vim.pretty_print(arguments)
      -- vim.pretty_print(arg_build(v))
      arg_text = table.append(arg_text, arg_build(v))
    end
  elseif arguments.dataType ~= nil then
    table.insert(arg_text, arguments.name .. " " .. arg_build(arguments.dataType)[1])
  elseif arguments.type ~= nil then
    -- mprint(arguments)
    table.insert(arg_text, arguments.name .. " " .. arg_build(arguments.type)[1])
  elseif arguments.typeKind ~= nil then
    if arguments.typeKind == "ARRAY" then
      arg_text[1] = "ARRAY<"
      arg_text[1] = arg_text[1] .. arg_build(arguments.arrayElementType)[1]
      arg_text[1] = arg_text[1] .. ">"
    elseif arguments.typeKind == "STRUCT" then
      arg_text[1] = "STRUCT<"
      arg_text[1] = arg_text[1] .. vim.fn.join(arg_build(arguments.structType.fields), ', ')
      arg_text[1] = arg_text[1] .. ">"
    else
      arg_text[1] = arguments.typeKind
    end
  end
  -- mprint(arg_text)
  return arg_text
end

M.edit_routines = function(bq_full_table)
	table_info = vim.split(bq_full_table, "%.")

	local bq_project = table_info[1]
	local bq_dataset = table_info[2]
	local bq_table = table_info[3]

	local url_info = "https://bigquery.googleapis.com/bigquery/v2/projects/"
		.. bq_project
		.. "/datasets/"
		.. bq_dataset
		.. "/routines/"
		.. bq_table

	local access_token
	access_toeken_job = Job:new({
		command = "gcloud",
		args = { "auth", "application-default", "print-access-token" },
		cwd = "/usr/bin",
		on_exit = function(j, return_val)
			access_token = j:result()[1]
		end,
	})
	access_toeken_job:after(function()
		curl.get(url_info, {
			headers = {
				Authorization = "Bearer " .. access_token,
			},
			callback = vim.schedule_wrap(function(out)
				local floating_buf = open_floating()
        local full_info_lines = {}
				local bqtable = vim.json.decode(out.body)
				local parsed_data = {
					{ "lastModifiedTime", timestamp_to_human(bqtable.lastModifiedTime) },
					{ "creationTime", timestamp_to_human(bqtable.creationTime) },
					{ "language", string.lower(bqtable.language) },
					{ "routineReference", vim.json.encode(bqtable.routineReference) },
					{ "arguments", vim.json.encode(bqtable.arguments)},
          { "returnType", vim.json.encode(bqtable.returnType)}
				}

				local pretty_parsed_lines = {}
				for _, v in ipairs(parsed_data) do
					if type(v[2]) == "table" then
						table.insert(pretty_parsed_lines, v[1] .. ": ")
						for k2, v2 in ipairs(v[2]) do
							table.insert(pretty_parsed_lines, "- " .. k2 .. ": " .. v2)
						end
					else
						table.insert(pretty_parsed_lines, v[1] .. ": " .. v[2])
					end
				end
				pretty_parsed_lines[#pretty_parsed_lines + 1] = ""
				pretty_parsed_lines[#pretty_parsed_lines + 1] = "```sql"

        local routine_type = nil
        local language_text = ""
        local returns_text = ""
        if bqtable.language == "JAVASCRIPT" then
          routine_type = "FUNCTION"
          returns_text = "RETURNS " .. vim.fn.join(arg_build(bqtable.returnType), ', ')
          language_text = [[LANGUAGE js AS R"""]]
        else
          routine_type = "PROCEDURE"
        end

        local args_text = vim.fn.join(arg_build(bqtable.arguments), ', ')

				pretty_parsed_lines[#pretty_parsed_lines + 1] = string.format("CREATE OR REPLACE %s `%s`(%s) %s %s", routine_type, bq_full_table, args_text, returns_text, language_text)


        table.append(pretty_parsed_lines, vim.split(bqtable.definitionBody, '\n'))

        if bqtable.language == "JAVASCRIPT" then
          pretty_parsed_lines[#pretty_parsed_lines + 1] = [["""]]
        end

				pretty_parsed_lines[#pretty_parsed_lines + 1] = "````"
				pretty_parsed_lines[#pretty_parsed_lines + 1] = ""

				vim.api.nvim_buf_set_lines(floating_buf, 0, -1, false, pretty_parsed_lines)
        table.append(full_info_lines, pretty_parsed_lines)

        local bqpath = Path:new(M.dir_bqtable)
        local path_dataset = (bqpath / bq_project / bq_dataset)
        pcall(path_dataset.mkdir, path_dataset, { parents = true })
        local saving_lines = vim.api.nvim_buf_get_lines(floating_buf, 0, -1, false)

        vim.fn.writefile(saving_lines, tostring(path_dataset / (bq_table .. ".md")), "b")
			end),
		})
	end)
	access_toeken_job:start()
end

M.ask_schema = function()
	local bq_full_table = ""

	vim.ui.input({ prompt = "Enter bigquery full path name: " }, function(input)
		bq_full_table = input
	end)

	pickers
		.new({}, {
			prompt_title = "Select sampling ratio:",
			results_title = "ratio",
			finder = finders.new_table({
				results = { "0.1", "1", "10", "50", "100" },
				entry_maker = function(entry)
					return {
						value = entry,
						display = entry,
						ordinal = entry,
					}
				end,
			}),
			sorter = conf.file_sorter({}),
			default_selection_index = 1,
			attach_mappings = function(prompt_bufnr)
				actions.select_default:replace(function()
					local selection = actions_state.get_selected_entry()
					actions.close(prompt_bufnr)
					print("You picked:", selection.display)
					M.show_schema(bq_full_table, selection.display)
				end)
				return true
			end,
		})
		:find()

	-- M.show_schema(bq_full_table, sampling_ratio)
end

M.delete_table = function(bq_full_table)
	table_info = vim.split(bq_full_table, "%.")

	local bq_project = table_info[1]
	local bq_dataset = table_info[2]
	local bq_table = table_info[3]

	local url_info = "https://bigquery.googleapis.com/bigquery/v2/projects/"
		.. bq_project
		.. "/datasets/"
		.. bq_dataset
		.. "/tables/"
		.. bq_table

	local access_token
	access_toeken_job = Job:new({
		command = "gcloud",
		args = { "auth", "application-default", "print-access-token" },
		cwd = "/usr/bin",
		on_exit = function(j, _)
			access_token = j:result()[1]
		end,
	})
	access_toeken_job:after(function()
		curl.delete(url_info, {
			headers = {
				Authorization = "Bearer " .. access_token,
			},
			callback = vim.schedule_wrap(function(out)
				if out.exit == 0 then
					print("Delete complete!")
				else
					print("Something wrong. please check")
				end
			end),
		})
	end)
	access_toeken_job:start()
end

M.ask_delete = function()
	local bq_full_table = ""
	vim.ui.input({ prompt = "!!!!!!!DELETE!!!!!!!! Enter bigquery full path name: " }, function(input)
		bq_full_table = input
	end)
	M.delete_table(bq_full_table)
end

local table_previewer = defaulter(function(opts)
	return previewers.new_buffer_previewer({
		title = "Description",
		get_buffer_by_name = function(_, entry)
			return entry.tableId
		end,
		define_preview = function(self, entry)
			local bufnr = self.state.bufnr
			local content = entry.value
			if type(entry.value) == "string" then
				content = vim.split(content, "\n")
			end
			vim.api.nvim_buf_set_lines(bufnr, 0, -1, false, content)
			vim.api.nvim_buf_set_option(bufnr, "filetype", "sql")
		end,
	})
end)

M.my_running_jobs = function(projectId, stateFilter, parentJobId)
	local url_info = "https://bigquery.googleapis.com/bigquery/v2/projects/"
		.. projectId
		.. "/jobs?maxResults=100&projection=FULL"

	local states_query
	if stateFilter ~= nil then
		states_query = table.concat(stateFilter, ",")
		url_info = url_info .. "&stateFilter=" .. states_query
	end

	local access_token
	access_toeken_job = Job:new({
		command = "gcloud",
		args = { "auth", "application-default", "print-access-token" },
		cwd = "/usr/bin",
		on_exit = function(j, _)
			access_token = j:result()[1]
		end,
	})
	access_toeken_job:after(function()
		curl.get(url_info, {
			headers = {
				Authorization = "Bearer " .. access_token,
			},
			callback = vim.schedule_wrap(function(out)
				local job_body = vim.json.decode(out.body)
				if job_body.jobs == nil then
					notify("There's no job", "info", { { title = "Bigquery Jobs" } })
					return
				end

				pickers
					.new({}, {
						prompt_title = string.format("Bigquery jobs - %s(%s)", projectId, vim.json.encode(stateFilter)),
						results_title = "Table",
						finder = finders.new_table({
							results = job_body.jobs,


							entry_maker = function(entry)

                local rval = ""
                if entry.configuration.query ~= nil then
                  if entry.configuration.query ~= nil then
                    rval = entry.configuration.query.query
                  end
                end

								return {
									value = rval,
									display = timestamp_to_human(tonumber(entry.statistics.creationTime)),
									ordinal = timestamp_to_human(tonumber(entry.statistics.creationTime)),
									project = rval,
									jobId = entry.jobReference.jobId,
									location = entry.jobReference.location,
								}
							end,
						}),
						previewer = table_previewer.new({}),
						sorter = conf.file_sorter({}),
						default_selection_index = 1,

						attach_mappings = function(prompt_bufnr)
							actions.select_default:replace(function()
								local selection = actions_state.get_selected_entry()
								actions.close(prompt_bufnr)
								print(
									string.format(
										"https://console.cloud.google.com/bigquery?project=%s&j=bq:%s:%s&page=queryresults",
										projectId,
										selection.location,
										selection.jobId
									)
								)
							end)
							return true
						end,
					})
					:find()
			end),
		})
	end)
	access_toeken_job:start()
end

M.ask_job = function(stateFilter)
	pickers
		.new({}, {
			prompt_title = "Select project:",
			results_title = "ratio",
			finder = finders.new_table({
				results = M.opts.configs.projects,
				entry_maker = function(entry)
					return {
						value = entry,
						display = entry,
						ordinal = entry,
					}
				end,
			}),
			sorter = conf.file_sorter({}),
			default_selection_index = 1,
			attach_mappings = function(prompt_bufnr)
				actions.select_default:replace(function()
					local selection = actions_state.get_selected_entry()
					actions.close(prompt_bufnr)
					print("You picked:", selection.display)
					M.my_running_jobs(selection.display, stateFilter)
				end)
				return true
			end,
		})
		:find()
end

local function show_job_body(job_body)
  local msg = {
    "## Created:" .. timestamp_to_human(job_body["statistics"]["creationTime"]),
    "",
		"## Updated: " .. os.date("%Y-%m-%d %H:%M:%S", os.time()),
    "",
		"## Job ID: " .. job_body["jobReference"]["jobId"],
    "",
		string.format(
			"https://console.cloud.google.com/bigquery?project=%s&j=bq:%s:%s&page=queryresults",
			job_body["jobReference"]["projectId"],
			job_body["jobReference"]["location"],
			job_body["jobReference"]["jobId"]
		),
    "",
		"## State: " .. job_body["status"]["state"],
    "## Entire: ",
    vim.json.encode(job_body),
	}
  if job_body["status"]["errorResult"] ~= nil then
    table.insert(msg, "- Error result: " .. job_body["status"]["errorResult"]["message"].. " / " .. job_body["status"]["errorResult"]["reason"])
  end

  vim.pretty_print(msg)

	return msg
end

M.async_bqjob = function(projectId, query_string)
	local resp_body = ""
	local access_token = ""

	access_toeken_job = Job:new({
		command = "gcloud",
		args = { "auth", "application-default", "print-access-token" },
		cwd = "/usr/bin",
		on_exit = function(j, return_val)
			access_token = j:result()[1]
		end,
	})
	access_toeken_job:after(function()
		local post_body = vim.json.encode({
			configuration = {
				query = {
					query = query_string,
					useLegacySql = false,
				},
			},
		})

		curl.post("https://bigquery.googleapis.com/bigquery/v2/projects/" .. projectId .. "/jobs", {
			headers = {
				Authorization = "Bearer " .. access_token,
				["Content-Type"] = "application/json",
				["Content-Length"] = post_body:len(),
			},
			body = post_body,
			callback = vim.schedule_wrap(function(out)
				resp_body = vim.json.decode(out.body)

				vim.cmd(string.format("vsplit running_%s.md", math.random(1000000)))

				split_buf = vim.api.nvim_get_current_buf()
				vim.api.nvim_buf_set_option(split_buf, "filetype", "markdown")
				vim.api.nvim_buf_set_lines(split_buf, 0, -1, false, { "## State: " .. resp_body["status"]["state"] })
			end),
		})
	end)
	access_toeken_job:start()

	local timer = vim.loop.new_timer()
	local i = 0
	timer:start(
		3000,
		10000,
		vim.schedule_wrap(function()
			if resp_body["jobReference"] ~= nil then
				print("timer invoked! i=" .. tostring(i))

				local url_info = "https://bigquery.googleapis.com/bigquery/v2/projects/"
					.. projectId
					.. "/jobs/"
					.. resp_body["jobReference"]["jobId"]

				local access_token
				job_body = ""
				access_toeken_job = Job:new({
					command = "gcloud",
					args = { "auth", "application-default", "print-access-token" },
					cwd = "/usr/bin",
					on_exit = function(j, _)
						access_token = j:result()[1]
					end,
				})

				access_toeken_job:after(function()
          if access_token ~= nil then
            curl.get(url_info, {
              headers = {
                Authorization = "Bearer " .. access_token,
              },
              callback = vim.schedule_wrap(function(out)
                job_body = vim.json.decode(out.body)
                vim.api.nvim_buf_set_lines(split_buf, 0, -1, false, show_job_body(job_body))
                if job_body["status"]["state"] == "DONE" then

                  if vim.g.focused == 0 then
                    io.popen([[
                    osascript -e 'display notification "Done" with title "Neovim Bigquery"'
                    ]])
                  else
                    notify("Done!", "info", { { title = "Bigquery Jobs" } })
                  end

                  pcall(timer.close, timer)
                end
              end),
            })
          end
				end)

				access_toeken_job:start()
			end

			if i > 90 then
				timer:close() -- Always close handles to avoid leaks.
				print("closed!")
				-- local tmp_job_body = show_job_body(job_body)
				-- table.insert(tmp_job_body, "- Loop closed!")
				-- vim.api.nvim_buf_set_lines(split_buf, 0, -1, false, tmp_job_body)
			end

			i = i + 1
		end)
	)

	return resp_body
end

M.async_bqjob_current_buf = function()
	local lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
	M.async_bqjob(M.opts.configs.default_project, table.concat(lines, "\n"))
end


M.setup = function(opts)
  M.opts = setmetatable(opts or {}, {__index = defaults})
  M.initialized = true

  if not Path:new(M.opts.config_path):exists() then 
    vim.notify('Bigquery is not initialized; please check the existance of config file.', 4)
    return
  end

  M.dir_bqtable = Path:new(os.getenv("HOME") .. "/.nvim/bqtable")

  if not dir_bqtable:is_dir() then
    M.dir_bqtable:mkdir()
  end

  -- read config_path which is json
  M.opts.configs = vim.json.decode(vim.fn.readfile(M.opts.config_path))

  if M.opts.configs.default_project == nil then
    vim.notify('Default project should not be empty', 4)
    return
  end


end

return M
