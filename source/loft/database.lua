-- ######################################### --
--  DATABASE ENGINE
-- ######################################### --

module(..., package.seeall)

--- Initializes the database engine and its closure-controled state
function init(engine, connection_params)
	local luasql, luasql_connect
	local db = _M
	local database_type = engine.options.database_type or database_type 
	local connection
	local cursors = {}
	local insertions = {}
	
	db.open_connection = db.open_connection or function()
		local luasql, err = luasql or require("luasql." .. database_type)
		local luasql_connect, err = luasql_connect or luasql[database_type]()
		connection, err = luasql_connect:connect(unpack(connection_params))
		
		return connection
	end  

	db.close_connection = db.close_connection or function()
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
	
	db.get_last_id = db.get_last_id or function(connection, ...)
		return assert(connection:execute(string.format(sql.LASTID, ...)))
	end

	db.last_id = db.last_id or function(...)
		local connection = connection or assert(db.open_connection())
		
		if not connection then
			error('Connection to the database could not be established')
		end
		
		return db.get_last_id(connection, ...) 
	end

	db.exec = db.exec or function(sql, ...)
		--TODO: think about connection closing strategies
		
		local params = {...}
		local connection = connection or assert(db.open_connection())
		
		if not connection then
			error('Connection to the database could not be established')
		end
		
		local cursor = assert(connection:execute(string.format(sql, ...)))
		
		if cursor and type(cursor)~='number' then
			local n = #cursors+1
			cursors[n]=cursor
			
			local value = cursor:fetch({},'a')
			if not value then
				cursor:close()
				cursors[n] = nil
			end

			-- returns an iterator function
			return function()
				local valueToReturn = value
				value = value and cursor:fetch({},'a')
				if not value then
					cursor:close()
					cursors[n] = nil
				end
				
				return valueToReturn
			end
		else
			return cursor
		end
	end
	
	return db
end