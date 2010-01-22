local cosmo = require 'cosmo'

require'util'

module("base", package.seeall)

description = [[Generic Module for Database Behaviour]]

reserved_words = {
	'and', 'fulltext', 'table'
}

filters = {
	
	like = function(s) 
		return string.gsub(s or '', '[*]', '%%')
	end,
	
	contains =function(f,s) 
		return string.format("CONTAIS(%s, %s)", f, s)
	end,

}

quotes = [[']]

escapes = {
	quotes = function(s) 
		return string.gsub(s or '', "'", "''")
	end,
	
	new_lines = function(s) 
		return string.gsub(s or '', "\n", "\\n")
	end,
	
	reserved_field_name =function(s)
		return string.format('`%s`', s) 
	end
	
}

---ToDo: Checar com o jason, a nova função do escape_field_name
escape_field_name=function(s)
	return reserved_words[string.lower(s)] and escapes.reserved_field_name(s) or s
end

string_literal=function(s) 
	return quotes .. escapes.quotes(escapes.new_lines(s)) .. quotes
end

field_types = {
	key={type='BIGINT', size='8', required=true, primary=true, autoincrement=true, onEscape=tonumber},
	integer={type='INT', size='5', onEscape=tonumber},
	number={type='DOUBLE', onEscape=tonumber},
	currency={type='DECIMAL', size={14,2}, onEscape=tonumber},
	text={type='VARCHAR',size='255', onEscape=string_literal},
	long_text={type='LONGTEXT', onEscape=string_literal},
	timestamp={type='DATETIME'}, 
	boolean={type='BOOLEAN'},
}

--  ]=]

sql = {
	
	CREATE = [==[CREATE TABLE IF NOT EXISTS $table_name ( 
			$columns{", "}[=[$escape_field_name{$column_name} $type$if{$size}[[($size)$if{$primary}[[ PRIMARY KEY]]]]$if{$required}[[ NOT NULL]]$if{$description}[[ COMMENT  $string_literal{$description}]]$if{$autoincrement}[[ AUTO_INCREMENT]]$sep
			]=])]==],
	
	INSERT = [[INSERT INTO $table_name ($data{", "}[=[$escape_field_name{$column_name}$sep]=]) VALUES ($data{", "}[=[$value$sep ]=]); SELECT LAST_INSERT_ID() as id]],
	
	UPDATE = [[UPDATE $table_name SET $data{", "}[=[$escape_field_name{$column_name}=$value$sep]=] WHERE id = $id]],
	
	SELECT = [==[SELECT ($columns{", "}[[$column_name as $name$sep]]) FROM $table_name $if{$filters}[=[WHERE ($filters_concat{" AND "}[[$it$sep]])]=] $if{$sorting}[=[ORDER BY $sorting_concat{", "}[[$it$sep]]]=] $if{$pagination}[[ LIMIT $pagination|limit OFFSET $pagination|offset]]]==],
	
	DELETE = [==[DELETE FROM $table_name $if{$filters}[=[WHERE ($filters_concat{" AND "}[[$it$sep]])]=]]==]
	
}

local passover_function = function(v) return v end

local field_type = function(field)
	local field_type_name = field.type
	return field_types[field_type_name] or {}
end

local table_fill_cosmo = function (engine, entity)
	local t ={}
	
	t.table_name = entity['table_name'] or entity['name']
	
	if ( not t.table_name ) then
		error("Entidade não possui o atributo 'table_name' ou 'name' no schema.")
	end
	
	local columns = {}
	
	for field_name, field in pairs(entity.fields) do
		local field_type = field_types[field.type] or {}
		local column = table.merge(field_type, field)
		
		column.name = field_name
		column.type = field_type.type
		columns[field_name] = column 
		
		table.insert(columns, columns[field_name])
	end
	
	t.__columns = columns
	t.columns = cosmo.make_concat( columns )
	
	t['string_literal'] = function (arg) 
		return string_literal(arg[1])
	end
	
	t['escape_field_name'] = function (arg) 
		return escape_field_name(arg[1])
	end
	
	t["if"] = function (arg)
	   if arg[1] then arg._template = 1 else arg._template = 2 end
	   cosmo.yield(arg)
	end
	
	return t
end

local filters_fill_cosmo = function (table_fill_cosmo, _filters)
	local t = table_fill_cosmo
	if ( type(_filters) == "table" and next(_filters)) then
		local lines = {}
		
		for name, val in pairs(_filters) do
			local col = t.__columns[name]
			
			if (not col) then error(name .. " não foi encontrado no schema") end
			local name = col.name
			local fn = col.onEscape or passoverFunction
			
			if type(val)=='string' or type(val)=='number' then
				table.insert(lines, col.column_name .. ' = ' .. fn(val))
			elseif type(val)=='table' then
				if #val>1 then
					local cp_val = table.copy(val)
					for i, v in ipairs(cp_val) do
						cp_val[i] = fn(v)
					end
					table.insert(lines, col.column_name .. ' IN (' .. table.concat(cp_val, ', ') .. ')')
				elseif val.contains then
					table.insert(lines, filters.contains(col.column_name, fn(val.contains)))
				elseif val.like then
					table.insert(lines, col.column_name .. ' LIKE ' .. fn(filters.like(val.like)) )
				elseif val.lt then
					table.insert(lines, col.column_name .. ' < ' .. fn(val.lt))
				elseif val.gt then
					table.insert(lines, col.column_name .. ' > ' .. fn(val.gt))
				elseif val.le then
					table.insert(lines, col.column_name .. ' <= ' .. fn(val.le))
				elseif val.ge then
					table.insert(lines, col.column_name .. ' >= ' .. fn(val.ge))
				end
			end
		end
		t.filters = {}
		t.filters_concat = cosmo.make_concat( lines )
	end
end
--- API

persist = function (engine, entity, id, obj)
	local t = table_fill_cosmo(engine, entity)
	obj.id = id or obj.id
	local data = {}
	local t_required = {}
	
	-- Faço checagem se todos os campos required estão no obj
	for i, column in ipairs(t.__columns) do
		
		if ( column.required ) then
			if ( not obj[ column.name ] and column.name ~= 'id') then
				table.insert( t_required, column.name )
			end
		end
		
		if ( obj[ column.name ] ) then
			local fn = column.onEscape or passoverFunction
			table.insert(data, {				
				column_name = column.name,
				value = fn( obj[ column.name ] )
			})
		end
		
	end
	
	if ( #t_required > 0 ) then		
		error("Os seguinte atributos precisam ser passados para o objeto (" .. table.concat(t_required, ',') .. ")")
	end
	
	t.data = cosmo.make_concat( data )
	t.id = obj.id
	
	if ( t.id ) then
		return cosmo.fill(sql.UPDATE, t)
	else
		return cosmo.fill(sql.INSERT, t)
	end
end

create = function (engine, entity)
	local t = table_fill_cosmo(engine, entity)
	return cosmo.fill(sql.CREATE, t)
end

delete = function (engine, entity, id, obj)	
	local t = table_fill_cosmo(engine, entity)	
	filters_fill_cosmo(t, { id = id })	
	return cosmo.fill(sql.DELETE, t)
end

search = function (engine, entity, _filters, pagination, sorting, visitorFunction)
	local t = table_fill_cosmo(engine, entity)
	
	if ( type(pagination) == "table" ) then
		t.pagination = pagination	
	end
	
	if ( type(sorting) == "table" and #sorting > 0 ) then
		local sortingcolumns = {}
		for i, v in ipairs(sorting) do
			local d = string.sub(string.gsub(v,'[^+%-]*', ''), 1, 1)
			local f = string.gsub(v,'[+%-]*', '')
			if d=='-' then
				table.insert( sortingcolumns, escape_field_name(f)..' DESC')
			else
				table.insert( sortingcolumns, escape_field_name(f)..' ASC')
			end
		end
		
		t.sorting = {}
		t.sorting_concat = cosmo.make_concat( sortingcolumns )
		
	end
	
	filters_fill_cosmo(t, _filters)

	return cosmo.fill(sql.SELECT, t)
end

retrieve = function (engine, entity, id)
	local t = table_fill_cosmo(engine, entity)	
	filters_fill_cosmo(t, { id = id })	
	return cosmo.fill(sql.SELECT, t)
end