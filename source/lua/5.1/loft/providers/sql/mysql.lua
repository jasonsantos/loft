----------------------------------------------
-- Loft MySQL SQL syntax adapter
----------------------------------------------

module(..., package.seeall)

local _M =  require "loft.providers.sql.generic"

function existTable(tableName)
	local cursor = exec("show tables like '%s'", tableName)
	if cursor:numrows() > 0 then
		cursor:close()
		return true
	end 
	if cursor then
		cursor:close()
	end
	return false
end

return _M