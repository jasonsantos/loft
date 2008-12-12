local sql = require 'loft.providers.sql.sqlite3'

----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 
module(..., package.seeall)

local _m = require 'loft.providers.database.generic'

local lfs = require'lfs'

local function isDir(path)
	local lfs = require 'lfs'
	return lfs.attributes(path, 'mode') == "directory"
end

local function dirName (path)
	return string.match(path, "^(.*)/[^/]*$")
end

local function mkdir(path)
	local parent = dirName (path)
	local result = true, msg 
	if not isDir(parent) then
		result, msg = mkdir(parent)
	end
	return result and lfs.mkdir(path), msg
end

_m.Private.defaultOptions.PERSISTENCE_PATH = './db/';
_m.Private.defaultOptions.PERSISTENCE_FILENAME = 'persistence.db3';

_m.createConnection = function (sourceName, ...)
	
	require"luasql.sqlite3"
	local fileName = (_m.options.PERSISTENCE_PATH or '') .. (sourceName or sourceName or _m.options.PERSISTENCE_FILENAME)
	mkdir(dirName(fileName))
	local env = luasql.sqlite3()

	_m.Private.environment = env
	_m.Private.connectionData = {fileName,...}
end

return _m