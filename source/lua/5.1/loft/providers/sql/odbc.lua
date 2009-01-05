----------------------------------------------
-- Loft MySQL SQL syntax adapter
----------------------------------------------

module(..., package.seeall)

local _m = require 'loft.providers.sql.generic'

function _m.createTable(tableName, structure)
	local sqlTable=setmetatable({},{__index=table}) 
	
	sqlTable:insert([[create table "]] .. tableName .. [["]])
	sqlTable:insert[[(]]

	local i
	structure.id = structure.id or 'Long'
	
	table.foreach(structure, function(field, type)
		sqlTable:insert(field .. '  ' .. (type or '') ..  ',')
		i = #sqlTable
	end)

	if i then
		sqlTable[i]=string.sub(sqlTable[i], 1, -2)
	end

	sqlTable:insert[[)]]
	return _m.exec(table.concat(sqlTable, '\n'))
end

function _m.select(tableName, id, filters)
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
				renderedAttribs = string.format("%s%s%s like '%s'", 
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
	local limit = (limit) and ('TOP ' .. limit ) or ''
	local sql = string.format('select %s * from %s %s %s %s %s', limit, tableName, where or '', filteredById or renderedAttribs or '', orderby, offset )

	local cursor, err = _m.exec(sql)
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

return _m