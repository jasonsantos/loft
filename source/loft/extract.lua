module(..., package.seeall)

run = function (engine, options) 
	local provider = engine.provider
	
	local cache_type = {}
	local convert_typebd_in_typeloft = function (type)
		if (cache_type[type]) then
			return cache_type[type]
		end
		for typeloft, t in pairs(provider.field_types) do
			if (t.type == type) then
				cache_type[type] = typeloft
				return cache_type[type]
			end
		end
		error("Não foi encontrado um tipo equivalente em loft " .. type)
	end
	
	local hook_regexp = {
		prefix = "^(%s).-",
		sufix = ".-(%s)$"
	}
	
	local apply_hook = function (value, entity)
		local value = value
		for op, regexp in pairs(hook_regexp) do
			local hook_name = entity .. "_" .. op
			local hook = options[hook_name]
			if not (not hook or hook == "") then 
				if (type(hook) == "function") then 
					value = hook(value)
				else
					value = string.gsub(value, string.format(regexp, hook), "")
				end
				if (not value or value == "") then
					error("O hook " .. hook_name .. " precisa retornar algum valor.")
				end
			end
		end
		return value
	end
	
	local get_fields_type_key = function (fields)
		local t = {}
		for _, t_field in ipairs(fields) do
			if (t_field.primary == true) then
				table.insert(t, t_field)
			end
		end
		return t
	end
	
	local t_result = {}
	for i, table_name in ipairs(provider.get_tables(engine)) do
		local real_table_name = table_name
		local fields = provider.get_description(engine, {table_name = real_table_name})
		local table_name = apply_hook(table_name, "table")
		local fields_type_key = get_fields_type_key(fields)
		if ( #fields_type_key == 1 ) then
			table.insert(t_result, string.format("%s = entity {", table_name))
			table.insert(t_result, string.format("	table_name = [[%s]],", real_table_name))
			table.insert(t_result, "	fields = {")
	
			for order, t_field in ipairs( fields ) do
				local type = convert_typebd_in_typeloft(t_field.type)
				local real_field = t_field.field
				local field = apply_hook(real_field, "column")
				
				if (t_field.primary == true) then
					type = "key"
					field = "id"
				end
		
				local field_options = {}
				if (t_field.required) then table.insert(field_options, " required = true") end
				if (t_field.size) then table.insert(field_options, " size = " .. t_field.size) end
				if (t_field.required) then table.insert(field_options, " required = true") end
		
				table.insert(t_result, string.format("		%s = { order = %d, column_name = [[%s]], type='%s',%s},", field, order, real_field, type, table.concat(field_options, ",")))
		
			end
	
			table.insert(t_result, "	}")
			table.insert(t_result, "}")
		end
	end
	return table.concat(t_result, "\n")
end