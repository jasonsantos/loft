local cosmo = require 'cosmo'

require'util'

----------------------------------------------
-- Persistence Provider for the Loft Module
----------------------------------------------
-- 

module(..., package.seeall)

description = [[Generic Module for Database Behaviour]]

-- ######################################### --
-- # API
-- ######################################### --
--  All persistence providers must implement 
--  this API to respond to the Loft engine
--  the 'base' provider will provide all of these 
--  interfaces, and you can extended them
--  in your own providers 
-----------------------------------------------

-- -------------- --
-- PROPERTIES
-- -------------- --

-- quotes
-- filters.like
-- filters.contains
-- escapes.quotes
-- escapes.new_lines
-- escapes.reserved_field_name

-- database_type
-- reserved_words
-- field_types
-- sql

-- -------------- --
-- FUNCTIONS
-- -------------- --

-- setup(engine)

-- create(engine, entity)
-- persist(engine, entity, id, data)
-- retrieve(engine, entity, id)
-- delete(engine, entity, id)

-- search(engine, options)
-- count(engine, options)



-- ######################################### --
--  OPTIONS
-- ######################################### --
--  this provider default options

local indexed_table_mt = {
	__newindex=function(t,k,v)
		if not tonumber(k) then
			table.insert(t, k)
		end
		rawset(t,k,v)
	end,
	__call=function(t,items)
		for _,v in ipairs(items) do
			t[v]=true
		end
	end
}

database_type = 'base'

reserved_words = setmetatable({}, indexed_table_mt)
 
reserved_words {
	'and',
	'fulltext', 
	'table'
}
 
filters = {
	
	like = function(s) 
		return string.gsub(s or '', '[*]', '%%')
	end,
	
	contains =function(f,s) 
		return string.format("CONTAINS(%s, %s)", f, s)
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

---ToDo: Checar com o jason, a nova fun��o do escape_field_name
local function contains_special_chars(s)
	return string.find(s, '([^a-zA-Z0-9_])')~=nil
end

escape_field_name=function(s)
	return (reserved_words[string.lower(s)] or contains_special_chars(s)) and escapes.reserved_field_name(s) or s
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
	
	CREATE = [==[
CREATE TABLE IF NOT EXISTS $table_name ( 
  $columns{", "}[=[$escape_field_name{$column_name} $type$if{$size}[[($size)$if{$primary}[[ PRIMARY KEY]]]]$if{$required}[[ NOT NULL]]$if{$description}[[ COMMENT  $string_literal{$description}]]$if{$autoincrement}[[ AUTO_INCREMENT]]$sep
]=])
]==],
	
	INSERT = [[INSERT INTO $table_name ($data{", "}[=[$escape_field_name{$column_name}$sep]=]) VALUES ($data{", "}[=[$value$sep ]=]); SELECT LAST_INSERT_ID() as id]],
	
	UPDATE = [[UPDATE $table_name SET $data{", "}[=[$escape_field_name{$column_name}=$value$sep]=] WHERE id = $id]],
	
	SELECT = [==[SELECT $columns{", "}[[$escape_field_name{$column_name} as $escape_field_name{$alias}$sep]] FROM $table_name $if{$filters}[=[WHERE ($filters_concat{" AND "}[[$it$sep]])]=] $if{$sorting}[=[ORDER BY $sorting_concat{", "}[[$it$sep]]]=] $if{$pagination}[[ LIMIT $pagination|limit OFFSET $pagination|offset]]]==],
	
	DELETE = [==[DELETE FROM $table_name $if{$filters}[=[WHERE ($filters_concat{" AND "}[[$it$sep]])]=]]==]
	
}

-- ######################################### --
--  INTERNALS
-- ######################################### --


local passover_function = function(...) return ... end

local field_type = function(field)
	local field_type_name = field.type
	return field_types[field_type_name] or {}
end

local table_fill_cosmo = function (engine, entity)
	local t ={}
	
	t.table_name = entity['table_name'] or entity['name']
	
	if ( not t.table_name ) then
		error("Entity must have a  `name` or `table_name`.")
	end
	
	local columns = {}
	
	for field_name, field in pairs(entity.fields) do
		local field_type = field_types[field.type] or {}
		local column = table.merge(field_type, field)
		
		column.name = field.column_name
		column.alias = field_name
		column.order = field.order or 999
		column.type = field_type.type
		columns[field_name] = column 
		
		table.insert(columns, columns[field_name])
	end
	
	table.sort(columns, function(f1,f2) return f1.order < f2.order end)
	
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
			
			if (not col) then error(name .. " not present in entity") end
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

-- ######################################### --
--  DATABASE ENGINE
-- ######################################### --


database_engine = {}

--- Initializes the database engine and its closure-controled state
function database_engine.init(engine, connection_params)
	local luasql
	local db = database_engine
	local connection
	local cursors = {}
	
	db.open_connection = function()
		local luasql = luasql or require("luasql." .. database_type)
		local engine = engine or luasql()
		local connection = connection or engine:connect(unpack(connection_params))
		
		return connection
	end  

	db.close_connection = function()
		local conn = connection
		connection = nil
		
		for idx,c in ipairs(cursors) do
			if c then
				c:close();
				cursors[idx]=nil
			end
		end
		conn:close()
	end  

	db.exec = function(sql, ...)
		--TODO: think about connection closing strategies
		
		local params = {...}
		local connection = connection or engine.open_connection()
		
		if not connection then
			error('Connection to the database could not be established')
		end
		
		local cursor = assert(connection:execute(string.format(sql, ...)))
		if type(cursor)~='number' then
			local n = #cursors+1
			cursors[n]=cursor
			
			local value = cursor:fetch({},'a')
			if not value then
				cursor:close()
				db.cursors[n] = nil
			end

			-- returns an iterator function
			return function()
				local valueToReturn = value
				value = cursor:fetch({},'a')
				if not value then
					cursor:close()
					db.cursors[n] = nil
				end
				
				return valueToReturn
			end
		end
	end
	
	return db
end

-- ######################################### --
--  PUBLIC API
-- ######################################### --

--- sets up specific configurations for this provider.
-- this function is executed when the engine is 
-- created. It can be used primarily to create 
-- the 'connection_string' or the 'connection_table' options from 
-- a more human-readable set of options    
-- @param engine the active Loft engine
-- @return alternative loft engine to be used or null if the original engine is to be used 
function setup(engine)
	engine.options.connection_table = {
		engine.options.database,
		engine.options.username,
		engine.options.password,
		engine.options.hostname,
		engine.options.port,
	}

	engine.db = database_engine.init(engine, engine.options.connection_table)	

	return engine
end

--- stores an instace of an entity onto the database
-- if the entity has an id, generates an update statement
-- otherwise, generates an insert statement
-- @param engine the active Loft engine
function persist(engine, entity, id, obj)
	local t = table_fill_cosmo(engine, entity)
	obj.id = id or obj.id
	local data = {}
	local t_required = {}
	
	-- Checking if every required field is present
	for i, column in ipairs(t.__columns) do
		
		if ( column.required ) then 
			if ( not obj[ column.alias ] and column.alias ~= 'id') then
				table.insert( t_required, column.alias )
			end
		end
		
		if ( obj[ column.alias ] ) then
			local fn = column.onEscape or passoverFunction
			table.insert(data, {				
				column_name = column.name,
				value = fn( obj[ column.alias ] )
			})
		end
		
	end
	
	if ( #t_required > 0 ) then		
		error("The following fields are absent (" .. table.concat(t_required, ',') .. ")")
	end
	
	t.data = cosmo.make_concat( data )
	t.id = obj.id
	
	local query
	if ( t.id ) then
		query = cosmo.fill(sql.UPDATE, t)
	else
		query = cosmo.fill(sql.INSERT, t)
	end
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local row, o = pcall(engine.db.exec(query))
	
	if row then
		--TODO: refresh object with other eventual database-generated values 
		obj.id = o.id or obj.id  
		return true, o.id
	else
		return null, o
	end 
end

function create(engine, entity)
	local t = table_fill_cosmo(engine, entity)
	local query = cosmo.fill(sql.CREATE, t)
	--TODO: proper error handling
	--TODO: think about query logging strategies
	return pcall(engine.db.exec(query))
end

--- Eliminates a record  from the persistence that corresponds to the given id 
-- @param engine the active Loft engine
-- @param entity the schema entity identifying the type of the object to remove
-- @param id identifier of the object to remove
-- @param obj the object itself
function delete(engine, entity, id, obj)	
	local t = table_fill_cosmo(engine, entity)	
	filters_fill_cosmo(t, { id = id })	
	
	local query = cosmo.fill(sql.DELETE, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	return pcall(engine.db.exec(query))
end

-- retrieve(engine, entity, id)
--- Obtains a table from the persistence that 
-- has the proper structure of an object of a given type
-- @param engine the active Loft engine
-- @param entity the schema entity identifying the type of the object to retrieve
-- @param id identifier of the object to load
-- @return object of the given type corresponding to Id or nil
function retrieve(engine, entity, id)
	local t = table_fill_cosmo(engine, entity)	
	filters_fill_cosmo(t, { id = id })	

	local query = cosmo.fill(sql.SELECT, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, query)
	
	if iter then
		return iter()
	else
		return null, iter
	end 
end

-- search(engine, options)
--- Perform a visitor function on every record obtained in the persistence through a given set of filters
-- @param engine the active Loft engine
-- @param options the
-- 			entity the schema entity identifying the type of the object to retrieve
-- 			filters table containing a set of filter conditions
-- 			pagination table containing a pagination parameters
-- 			sorting table containing a sorting parameters
-- 			visitor	(optional) function to be executed 
-- 					every time an item is found in persistence
--					if ommited, function will return a list with everything it found
-- @return 			array with every return value of the resultset, after treatment by the visitor 
function search(engine, options)
	local entity, _filters, pagination, sorting, visitorFunction =
 		(options.entity or options[1]), options.filters, options.pagination, options.sorting, options.visitor
 		
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

	local query = cosmo.fill(sql.SELECT, t)
	
	--TODO: proper error handling
	--TODO: think about query logging strategies
	local ok, iter = pcall(engine.db.exec, query)
	
	if iter then
		--TODO: implement resultset proxies using the list module
		local results = {}
		local fn = visitorFunction or passover_function
		local row = iter() 
		while row do
			local o = fn(row)
			table.insert(results, o)		
			row = iter()
		end
		return results
	else
		return null, iter
	end 
end

-- count(engine, options)
--- Gets the number of results of a given set of search options 
-- @param engine the active Loft engine
-- @param options the
-- 			entity the schema entity identifying the type of the object to retrieve
-- 			filters table containing a set of filter conditions
-- @return 			number of results to be expected with these options

function count(engine, options)
 	local entity, _filters, pagination, sorting, visitorFunction =
 		options.entity, options.filters, options.pagination, options.sorting, options.visitor
 		
	local t = table_fill_cosmo(engine, entity)
	
	filters_fill_cosmo(t, _filters)

	t.columns = { 'COUNT(*)' }

	local query = cosmo.fill(sql.SELECT, t)
	
	local ok,num = pcall(engine.db.exec(query))
	
	return ok and num or null, num
end