local sql = require"loft.providers.sql.generic"

local CON_CLOSE = false
local CON_SQL = ""
local CON_T_SQL = {}
local CON_T_SQL_SMART = {}
local T_SQL_VALID = {}

connection = {
	execute = function (self, sql) 
		
		local context = nil
		
		CON_SQL = string.lower(sql)
		string.gsub(CON_SQL, "(%w+)", function (v) 
			table.insert(CON_T_SQL, v)
		end)
		for i, _s in ipairs(T_SQL_VALID) do
			if (_s.sql == CON_SQL) then
				context = _s
				break
			end
		end
		
		if (not context) then
			return nil
		end
				
		return {
			close = function (self)
				CON_CLOSE = true
			end,
			fetch = coroutine.wrap(function ()
					if (not context.fetch) then
						return nil
					end
					for i, v in ipairs(context.fetch) do
						coroutine.yield(v)					
					end
					return nil
				end),
			getcolnames = function (self)			
			end,
			getcoltypes = function (self)			
			end,
			numrows = function (self)			
			end
		}
	end,
}



assert( sql.initialize(connection) == nil )

assert( sql.exec("select * from test") == nil )
assert( CON_SQL == "select * from test" )
assert( CON_CLOSE == false )

assert( sql.exists("test", 1) == nil)
assert( CON_SQL == "select id from test where id = 1" )

local fetch_test = {
	{ 
		id = 1
	}
}

table.insert(T_SQL_VALID, {sql = "select id from test where id = 1", fetch = fetch_test } )
table.insert(T_SQL_VALID, {sql = "select id from test where id = 1   ", fetch = fetch_test } )
table.insert(T_SQL_VALID, {sql = "select * from test where id in (1)   ", fetch = fetch_test } )

assert( type(sql.exec("select id from test where id = 1   ")) == "table" )
assert( sql.exists("test", 1) )
assert( CON_SQL == "select id from test where id = 1" )
assert( sql.exists("test") == nil)


local _result = sql.select("test", 1, { id = 1})
assert(type(_result) == "table")
assert(type(_result[1]) == "table")
assert( _result[1].id == 1 )
assert( CON_SQL == "select * from test where id in (1)   " )

local _result = sql.select("test", 2, { id = 1})
assert( CON_SQL == "select * from test where id in (2)   " )
assert(type(_result) == "nil")

--insert
--update
--delete

