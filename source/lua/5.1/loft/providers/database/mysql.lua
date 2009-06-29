local sql = require 'loft.providers.sql.mysql'

----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 
module(..., package.seeall)

local _m = require 'loft.providers.database.generic'

_m.Private.defaultOptions.HOSTNAME = "localhost";
_m.Private.defaultOptions.USERNAME = nil;
_m.Private.defaultOptions.PASSWORD = nil;
_m.Private.defaultOptions.PORT = "3304";
_m.Private.defaultOptions.SOURCENAME = nil;

_m.createConnection = function (sourceName, ...)
	require"luasql.mysql"
	local env = luasql.mysql()
	
	_m.Private.environment = env
	_m.Private.connectionData = {sourceName,...}
end

return _m