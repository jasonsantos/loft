----------------------------------------------
-- Loft ACCESS SQL syntax adapter
----------------------------------------------

module(..., package.seeall)

local _m = require 'loft.providers.sql.odbc'

function _m.createTable(tableName, structure)
	local sqlTable=setmetatable({},{__index=table}) 
	
	sqlTable:insert[[create table ]]
	sqlTable:insert([[`]] .. tableName .. [[`]])
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

return _m