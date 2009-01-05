local sql = require 'loft.providers.sql.odbc'

----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 
module(..., package.seeall)

local _m = require 'loft.providers.database.generic'

_m.Private.defaultOptions.USERNAME = nil;
_m.Private.defaultOptions.PASSWORD = nil;
_m.Private.defaultOptions.SOURCENAME = nil;

_m.createConnection = function (sourceName, ...)
	require"luasql.odbc"
	local env = luasql.odbc()

	_m.Private.environment = env
	_m.Private.connectionData = {sourceName,...}
end

return _m