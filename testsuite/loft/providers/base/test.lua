package.path = package.path .. ";;../../../../source/?.lua"

local schema = require'schema'

package.preload['luasql.base']= function()
	return {
		connect = function(...)
			
		end,
	}
end

local default = schema.expand(function ()
	column_prefix = "f_"

	info = entity {
		table_name= "T_Info",
		fields = { 
			id = { order = 1, column_name = "f_infoid", type = "key", required=true},
			title = { order=2, type = "text", size = 100, maxlength=250 },
			summary=long_text{order=3},
			fulltext=long_text(),							
			section = text(),
			authorName = text(),
			authorMail = text(),
			actor = text(),
			creatorActor = text(),
			state = integer{
				size=10, description="Estado da info, pode assumir um de '5' valores",
				handlers = {
	                get = function (f, v) record.gettings = record.gettings + 1 return v end,
	                set = function (f, v) record.settings = record.settings + 1 return v end,
	            }
			}
		},
		handlers = {
	        before_save = function(e, obj) record.lastLog = 'Saving Entity' end,
	    },
	}
	
	section = entity { 
		fields = { 
			id = { order = 1, colum_name = "F_SectionID", type = "key" },
			name = { type = "text", size = 100, maxlength=250 },
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}	
	
end)

local provider = require"loft.providers.base"

-- query testing apparatus

local queries = {}

local function short_query(sql)
	sql = string.gsub(sql, "%s+", " ") 
	sql = string.gsub(sql, "^%s*", "") 
	sql = string.gsub(sql, "%s*$", "")
	return sql 
end

local function assert_last_query(sql)
	if short_query(sql)~=short_query(queries[#queries]) then
		error("assertion failed", 2)
	end
end

local engine = {
	db = {
		exec = function(sql)
			local result = {
				{id=1, summary="test summary content", fulltext="test fulltext content"}
			}
		--print'----------------------------'
		--print(sql)
		--print'----------------------------'
			table.insert(queries, sql)
			return function()
				return table.remove(result, #result)
			end 
		end
	}
} 

provider.create(engine, default.entities.info)

assert_last_query[[
CREATE TABLE IF NOT EXISTS T_Info ( 
  f_infoid BIGINT(8) PRIMARY KEY NOT NULL AUTO_INCREMENT,
  f_title VARCHAR(100),
  f_summary LONGTEXT,
  f_fulltext LONGTEXT,
  f_section VARCHAR(255),
  f_authorName VARCHAR(255),
  f_authorMail VARCHAR(255),
  f_actor VARCHAR(255),
  f_creatorActor VARCHAR(255),
  f_state INT(10) COMMENT  'Estado da info, pode assumir um de ''5'' valores'
)

]]

local obj = {
	summary = "Resumo",
	fulltext = "Texto",
	authorName = "autor"
}

provider.persist(engine, default.entities.info, nil, obj) 

assert_last_query[[
INSERT INTO T_Info (f_summary, f_fulltext, f_authorName) VALUES ('Resumo', 'Texto', 'autor' ); SELECT LAST_INSERT_ID() as id]]

assert(obj.id==1)

provider.persist(engine, default.entities.info, 1, {
	summary = "Resumo",
	id = 1,
	fullText = "Texto",
	authorName = "autor"
})

assert_last_query[[
UPDATE T_Info SET f_infoid=1, f_summary='Resumo', f_authorName='autor' WHERE id = 1
]]


local o = provider.retrieve(engine, default.entities.info, 1)

assert_last_query[[
SELECT 
	f_infoid as id, 
	f_title as title, 
	f_summary as summary, 
	f_fulltext as `fulltext`, 
	f_section as section, 
	f_authorName as authorName, 
	f_authorMail as authorMail, 
	f_actor as actor, 
	f_creatorActor as creatorActor, 
	f_state as state 
	FROM T_Info 
	WHERE (f_infoid = 1) 
]]

assert(o.id == 1)
assert(o.summary == 'test summary content')
assert(o.fulltext == 'test fulltext content')

provider.delete(engine, default.entities.info, 1)

assert_last_query[[
DELETE FROM T_Info WHERE (f_infoid = 1)
]]

provider.search(engine, {
	default.entities.info, 
	filters = {
		id = 1,
		state = {1, 2, 3, 4}
	}
	}) 

assert_last_query[[
SELECT 
	f_infoid as id, 
	f_title as title, 
	f_summary as summary, 
	f_fulltext as `fulltext`, 
	f_section as section, 
	f_authorName as authorName, 
	f_authorMail as authorMail, 
	f_actor as actor, 
	f_creatorActor as creatorActor, 
	f_state as state 
	FROM T_Info 
	WHERE (f_infoid = 1 AND f_state IN (1, 2, 3, 4)) 
]]


os.exit()

provider.search(engine, {
	default.entities.info, 
	title = 'aaaa',
	authorName = { like = "b"},
	id = 1,
	state = {1, 2, 3, 4},
	summary = { gt = "a" }
	}) 

print( provider.search(engine, default.entities.info, { 
	
	}) 
)

print( provider.search(engine, default.entities.info, nil))

print( provider.search(engine, default.entities.info, nil, { limit = 1, offset = 1 }) )

print( provider.search(engine, default.entities.info, nil, nil, {"title+", "-id", "+fulltext+"}) )