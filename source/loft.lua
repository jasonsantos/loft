require "util"

module("loft",package.seeall)

local cache_pool = {}
local register

-- Configuration API
-- ----------------- --

local defaults = {}

-- add default values to missing options
local function prepare(options)
	return table.merge(defaults, options)
end

--- register default values for loft.engine
function configure(options)
	local defaults = table.add(defaults, options)
	return table.copy(defaults)
end

-- Loft Engine Public API Table
local api = {}

--- creates a new loft engine with its own options table
function engine(options)
	local options = prepare(options)
	local engine = table.add({options=options}, public)
	--TODO: startup the engine
	return engine
end

-- Plugin API
-- ----------------- --

local plugin_api = {}

function plugin_api.add(plugin)
	if not (plugin.name and plugin.configure and plugin.run) then
		error"Invalid Plugin structure: plugins must have a name and a configure and run functions"
	end
	plugins[plugin.name] = plugin
end


--- loft.plugins 
-- a way to find all registered plugins
plugins = setmetatable({}, {__index=plugin_api})

-- Engine API
-- ----------------- --

--- Creates a new instance of an object
-- alternatively, can turn a simple table into an object
-- can also be used to recreate objects from their data tables
-- @param entity	schema entity object or its name
--					if no entity is designated, a simple table object will be created
-- @param data 		(optional) table with data to be loaded into object
-- @param id 		(optional) ID of the object to be restored
-- @return new object of the designated type or a simple object
function api.new(entity, data, id)
	error'not implemented'
end

--- Saves the object to the persistence.
-- if object has a complex type, saves to the appropriate repository
-- if object has a simple type, saves according to the object ID
-- @param obj object to be saved
-- @param force boolean indicating whethe the object is to be saved even if it's not changed
-- @return boolean indicating whether the object needed to be saved or not (i.e. if it was changed since its last)
function api.save(obj, force)
	error'not implemented'
end


--- Recovers an object by its ID.
-- if object is already in memory cache and its time_to_live is still valid, it is obtained directly from there
-- if not, it will be loaded from persistence and restored to memory cache
-- @param entity 	schema entity of the object to be retrieved
-- @param id 		ID of the object to be retrieved
-- @return 			object recovered
function api.get(entity, id)
	error'not implemented'
end

--- Destroys an object.
-- remove it from memory and persistence. 
--
-- @param obj object to be destroyed
-- @return true if object was successfully erased from persistence
function api.destroy(obj)
	error'not implemented'
end

--- Finds a list of objects matching a given set of filters.
-- foreach given object matching the criteria, if it is already 
-- in memory cache, it is obtained directly from there
-- if not, it will be loaded from persistence and restored to memory cache
--
-- @param entity 	schema entity of the objects to be retrieved
-- @param options	table containing the criteria for the retrieval of objects
-- 
--  entity			alternate place to put the entity param 
-- 
--  order			array containing a list of fields to be used in the sorting clauses 
-- 
--  filter		 	table containing a set of filter conditions
--					filters are tables with keys representing fieldnames
--					and their correspontant values can be either strings 
--					(when you want to filter by equalty to a specific value)
-- 					arrays (when you want to indicate multiple possible values)
--					or tables (when you want to indicate a distinct comparison operation).
--					Ex.: { nome = "fulano", state = {1, 4, 6}, {like= '%manager%'} } 
--					
--	visitor			function to be executed every time an item is found in persistence
--
-- @return 			list with all objects recovered

function api.find(entity, options)
	error'not implemented'
end

function api.decorate(schema, options)
	error'not implemented'
end
