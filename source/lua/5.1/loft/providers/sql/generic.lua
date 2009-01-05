----------------------------------------------
-- Loft Generic SQL syntax adapter
----------------------------------------------

-- TODO: internationalization of messages

module(..., package.seeall)

local connection

function initialize(conn)
	connection = conn
end

--- directly executes an SQL statement using simple parameters
--	@params	sql	sql instruction to execute. Must have proper placeholders for string.format
--	@params ...	paremeters to be used in string.format
--	@return query result
function exec(sql, ...)
	local s = string.format(sql, ...)
	return connection:execute(s)
end

function exists(tableName, id)
	if (not id) then
		return nil, "second argument is nil"
	end
	
	local cursor, err = exec('select id from %s where id = %d', tableName, id) 
	local row = {}
	if (cursor) then	
		row = cursor:fetch(row)
		cursor:close()
		return row and row[1] or row
	else
		return nil, err
	end
end

function select(tableName, id, filters)
	local filteredById
	local renderedAttribs
	local filters = filters or {}
	
	local orderby = filters['__sort']
	local offset = filters['__offset']
	local limit = filters['__limit']
	
	local orderbyfields = {}
	local add = function(asc, field)
		asc = asc=='-' and ' DESC' or asc=='+' and ' ASC' or '' 
		table.insert(orderbyfields, field .. asc )
	end
	
	if type(orderby) == 'string' then
		string.gsub(orderby, '([+-]?)(%w+)', add)
	elseif type(orderby) == 'table' then
		table.foreachi(orderby, function(_, field)
			string.gsub(field, '([+-]?)(%w+)', add)
		end);
	end
	
	table.foreach(filters, function(field, value)
		if string.sub(field, 1, 1) ~= '_' then
			if type(value) == 'table' then
				renderedAttribs = string.format("%s%s%s %s '%s'", 
					renderedAttribs or '', 
					renderedAttribs and ' AND ' or '',
					field, value.__operator, value[1] )
			else
				renderedAttribs = string.format("%s%s%s='%s'", 
					renderedAttribs or '', 
					renderedAttribs and ' AND ' or '',
					field, value )
			end
			
		end
	end)

	
	if tonumber(id) then
		id = {id}
	end 

	if type(id)=='table' then
		local ids 
		
		table.foreach(id, function(_,item)
			ids = string.format("%s%s%d", 
				ids or '',
				ids and ', ' or '',
				tonumber(item))
		end)
		filteredById = ids and string.format('ID in (%s)', ids) 
	end
	
	local where = (renderedAttribs or filteredById) and 'WHERE'
	local orderby = (#orderbyfields > 0) and ('ORDER BY ' .. table.concat(orderbyfields, ',')) or ''
	local offset = (offset) and ('OFFSET ' .. offset ) or ''
	local limit = (limit) and ('LIMIT ' .. limit ) or ''
	local sql = string.format('select * from %s %s %s %s %s %s', tableName, where or '', filteredById or renderedAttribs or '', orderby, offset, limit)

	local cursor, err = exec(sql)
	if (cursor) then
		local row={}
		local list={}
					
		while row do
			row = cursor:fetch({}, '*a')
			table.insert(list, row) 
		end
		cursor:close()
	
		return list
	else
		return nil, err
	end
end

function insert(tableName, data)
	local renderedFields
	local renderedValues
	
	table.foreach(data, function(field, value)
		if string.sub(field, 1, 1) ~= '_' then
			renderedFields = string.format("%s%s%s", 
				renderedFields or '', 
				renderedFields and ', ' or '',
				field)
			
			renderedValues = string.format("%s%s'%s'", 
				renderedValues or '', 
				renderedValues and ', ' or '',
				string.gsub(value, "'", "''"))
		end
	end)
	

	local ok, msg = exec('insert into %s (%s) values (%s)', tableName, renderedFields, renderedValues)
	return ok, msg or (ok and 'Registro inserido com sucesso')
end

function update(tableName, id, data)
	local renderedAttribs
	local filteredById
	
	table.foreach(data, function(field, value)
		if string.sub(field, 1, 1) ~= '_' then
			renderedAttribs = string.format("%s%s%s='%s'", 
				renderedAttribs or '', 
				renderedAttribs and ', ' or '',
				field, string.gsub(value, "'", "''") )
		end
	end)

	if tonumber(id) then
		id = { tonumber(id) }
	end
	
	if type(id)=='table' then
		local ids 
		table.foreach(id, function(_,item)
			ids = string.format("%s%s%d", 
				ids or '',
				ids and ', ' or '',
				tonumber(item))
		end)
		filteredById = string.format('WHERE id in (%s)', ids)
	end

	local ok, msg = exec('update %s set %s %s', tableName, renderedAttribs, filteredById or '')
	return ok, msg or (ok and tostring(ok) .. ' Registro(s) atualizado(s) com sucesso')
end

function delete(tableName, id, data)
	local filteredById
	
	if tonumber(id) then
		id = {id}
	end 

	if type(id)=='table' then
		local ids 
		
		table.foreach(id, function(_,item)
			ids = string.format("%s%s%d", 
				ids or '',
				ids and ', ' or '',
				tonumber(item))
		end)
		filteredById = string.format('(%s)', ids)
	else
		return false, 'Precisa fornecer o ID ou a lista de IDs para deleção'
	end
	
	local ok, msg = exec('delete from %s WHERE id in %s', tableName, filteredById)
	
	return ok, msg or (ok and tostring(ok) .. ' Registro(s) deletados(s) com sucesso')
end

--- Creates a table using SQL
function createTable(tableName, structure)
	local sqlTable=setmetatable({},{__index=table}) 
	
	sqlTable:insert[[create table if not exists]]
	sqlTable:insert(tableName)
	sqlTable:insert[[(]]

	local i
	structure.id = structure.id or 'INT'
	
	table.foreach(structure, function(field, type)
		sqlTable:insert(field .. '  ' .. (type or '') ..  ',')
		i = #sqlTable
	end)

	if i then
		sqlTable[i]=string.sub(sqlTable[i], 1, -2)
	end

	sqlTable:insert[[)]]
	return exec(table.concat(sqlTable, '\n'))
end

function existTable(tableName)
	local cursor = exec("select NULL from %s", tableName)
	if cursor then
		local result = true
		cursor:close()
		return true
	end 
	return false
end

-----------------------------------------------------------------
