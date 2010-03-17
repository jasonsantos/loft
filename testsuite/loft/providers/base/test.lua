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
			section = belongs_to{'section'},
			authorName = text(),
			authorMail = text(),
			author = belongs_to{'actor'},
			creatorActor = belongs_to{'actor'},
			state = integer{
				size=10, description="Info state, can assume one of '5' values",
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
		table_name= "T_Section", 
		fields = { 
			id = { order = 1, column_name = "F_SectionID", type = "key" },
			name = { type = "text", size = 100, maxlength=250 },
			tag = { type = "text", size = 100, maxlength=250 },
			editor = has_one{ 'editor' },
			infos = has_many{ 'info' }
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}	
	
	editor = entity {
		table_name= "T_Editor", 
		fields = { 
			id = key{},
			actor = belongs_to{ column_name = "F_ActorID", entity = "actor" },
			sections = has_many{ 'section' }
		},
		handlers = {
	        before_save = function(e, obj) print('!!!!!!!!') end,
	    },
	}	
	
	actor = entity {
		table_name= "T_Actor", 
		fields = { 
			id = { order = 1, column_name = "F_ActorID", type = "key" },
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
	schema = default,
	provider = provider,
	db = {
		exec = function(sql)
			local result = {
				{id=1, summary="test summary content", fulltext="test fulltext content"}
			}
		print'----------------------------'
		print("'"..sql.."'")
		print'----------------------------'
			table.insert(queries, sql)
			return function()
				return table.remove(result, #result)
			end 
		end,
		
		last_id = function()
			return 1
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
  f_section_id BIGINT(8),
  f_authorName VARCHAR(255),
  f_authorMail VARCHAR(255),
  f_author_id BIGINT(8), 
  f_creatorActor_id BIGINT(8), 
  f_state INT(10) COMMENT  'Info state, can assume one of ''5'' values'
);
]]

local obj = {
	summary = "Resumo",
	fulltext = "Texto",
	authorName = "autor"
}

provider.persist(engine, default.entities.info, nil, obj) 

assert_last_query[[
INSERT INTO T_Info (f_summary, f_fulltext, f_authorName) VALUES ('Resumo',  'Texto',  'autor' )]]

assert(obj.id==1)

provider.persist(engine, default.entities.info, 1, {
	summary = "Resumo",
	id = 1,
	fullText = "Texto",
	authorName = "autor"
})

assert_last_query[[
UPDATE T_Info SET f_infoid=1, f_summary='Resumo', f_authorName='autor' WHERE (f_infoid = 1)]]

local o = provider.retrieve(engine, default.entities.info, 1)

assert_last_query[[
SELECT 
	f_infoid as id, 
	f_title as title, 
	f_summary as summary, 
	f_fulltext as `fulltext`, 
	f_section_id as section, 
	f_authorName as authorName, 
	f_authorMail as authorMail, 
	f_author_id as author, 
    f_creatorActor_id as creatorActor,
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

provider.search(engine, {default.entities.info}) 

assert_last_query[[
SELECT 
	f_infoid as id, 
	f_title as title, 
	f_summary as summary, 
	f_fulltext as `fulltext`, 
	f_section_id as section, 
	f_authorName as authorName, 
	f_authorMail as authorMail, 
	f_author_id as author, 
    f_creatorActor_id as creatorActor,
	f_state as state 
	FROM T_Info 
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
	f_section_id as section, 
	f_authorName as authorName, 
	f_authorMail as authorMail, 
	f_author_id as author, 
    f_creatorActor_id as creatorActor,
	f_state as state 
	FROM T_Info 
	WHERE (f_infoid = 1 AND f_state IN (1, 2, 3, 4)) 
]]

provider.search(engine, {
	default.entities.info, 
	filters = {
		id = 1,
		state = {notin = {1, 2, 3, 4}}
	}
	}) 

assert_last_query[[
SELECT 
	f_infoid as id, 
	f_title as title, 
	f_summary as summary, 
	f_fulltext as `fulltext`, 
	f_section_id as section, 
	f_authorName as authorName, 
	f_authorMail as authorMail, 
	f_author_id as author, 
    f_creatorActor_id as creatorActor,
	f_state as state 
	FROM T_Info 
	WHERE (f_infoid = 1 AND f_state NOT IN (1, 2, 3, 4)) 
]]

provider.search(engine, {
	default.entities.info, 
	filters = {
		title = {notnull=true},
		authorName = { like = "a*"},
		state = { gt = 3 }
	}
}) 

assert_last_query[[
SELECT 
 f_infoid as id, 
 f_title as title, 
 f_summary as summary, 
 f_fulltext as `fulltext`, 
 f_section_id as section, 
 f_authorName as authorName, 
 f_authorMail as authorMail, 
 f_author_id as author, 
 f_creatorActor_id as creatorActor,
 f_state as state
FROM T_Info
WHERE (f_state > 3 AND f_title IS NOT NULL AND f_authorName LIKE 'a%')  ]]

provider.search(engine, { default.entities.info }) 

assert_last_query[[
SELECT 
		 f_infoid as id, 
		 f_title as title, 
		 f_summary as summary, 
		 f_fulltext as `fulltext`, 
		 f_section_id as section, 
		 f_authorName as authorName, 
		 f_authorMail as authorMail, 
		 f_author_id as author, 
	     f_creatorActor_id as creatorActor,
		 f_state as state
		FROM T_Info
]]

provider.search(engine, { default.entities.info, pagination= {limit = 1, offset = 1 }}) 

assert_last_query[[
SELECT 
		 f_infoid as id, 
		 f_title as title, 
		 f_summary as summary, 
		 f_fulltext as `fulltext`, 
		 f_section_id as section, 
		 f_authorName as authorName, 
		 f_authorMail as authorMail, 
		 f_author_id as author, 
    	 f_creatorActor_id as creatorActor,
		 f_state as state
		 FROM T_Info
		   LIMIT 1  OFFSET 1
]]

provider.search(engine, {default.entities.info, sorting={"title+", "-id", "+fulltext+"}}) 

assert_last_query[[
SELECT 
		 f_infoid as id, 
		 f_title as title, 
		 f_summary as summary, 
		 f_fulltext as `fulltext`, 
		 f_section_id as section, 
		 f_authorName as authorName, 
		 f_authorMail as authorMail, 
		 f_author_id as author, 
	     f_creatorActor_id as creatorActor,
		 f_state as state
		FROM T_Info
		 ORDER BY title ASC, id DESC, `fulltext` ASC 
]]

provider.search(engine, {
	default.entities.info, 	
	filters = {
		title = {notnull=true},
		authorName = { like = "a*"},
		state = { gt = 3 }
	},
	pagination= {limit = 1, offset = 1 }, 
	sorting={"title+", "-id", "+fulltext+"}
}) 

assert_last_query[[
SELECT 
   f_infoid as id, 
   f_title as title, 
   f_summary as summary, 
   f_fulltext as `fulltext`, 
   f_section_id as section, 
   f_authorName as authorName, 
   f_authorMail as authorMail, 
   f_author_id as author, 
   f_creatorActor_id as creatorActor,
   f_state as state
  FROM T_Info
  WHERE (f_state > 3 AND f_title IS NOT NULL AND f_authorName LIKE 'a%') ORDER BY title ASC, id DESC, `fulltext` ASC  LIMIT 1  OFFSET 1
]]

provider.search(engine, {
	default.entities.info,
	exclude_fields = {
		'title','summary', 'fulltext'
	}, 	
	filters = {
		title = {notnull=true},
		fulltext = { contains = "*shake*"},
	},
	sorting={"title+"}
}) 

assert_last_query[[
SELECT 
   f_infoid as id, 
   f_section_id as section, 
   f_authorName as authorName, 
   f_authorMail as authorMail, 
   f_author_id as author, 
   f_creatorActor_id as creatorActor,
   f_state as state
  FROM T_Info
  WHERE (f_title IS NOT NULL AND CONTAINS(f_fulltext, '*shake*')) ORDER BY title ASC
]]

provider.search(engine, {
	default.entities.info,
	include_fields = {
		'id','title'
	}, 	
	filters = {
		state = { 1, 2 },
	},
	sorting={"title+"}
}) 

assert_last_query[[
SELECT 
   f_infoid as id, 
   f_title as title
  FROM T_Info
  WHERE (f_state IN (1, 2)) ORDER BY title ASC
]]

provider.create(engine, default.entities.section)

assert_last_query[[
CREATE TABLE IF NOT EXISTS T_Section ( 
  F_SectionID BIGINT(8) PRIMARY KEY NOT NULL AUTO_INCREMENT, 
f_editor_id BIGINT(8), 
f_name VARCHAR(100), 
f_tag VARCHAR(100)
);
]]


provider.search(engine, {
	default.entities.info,
	include_fields = {
		'id'
	}, 	
	filters = {
		state = {eq={field='id'}},
	}
}) 

assert_last_query[[
SELECT 
   f_infoid as id
  FROM T_Info
  WHERE (f_state = f_infoid) 
]]


provider.search(engine, {
	default.entities.info,
	include_fields = {
		'id'
	}, 	
	filters = {
		section_name = 'teste',
	}
}) 

assert_last_query[[
SELECT 
   f_infoid as id
  FROM T_Info
  info INNER JOIN T_Section section ON ( info.f_section_id = section.F_SectionID )  
  WHERE (section.f_name = 'teste')
]]

provider.search(engine, {
	default.entities.info,
	include_fields = {
		'id'
	}, 	
	filters = {
		section_name = 'Ancient Rome',
		author_name = 'Plutarch',
	}
}) 

assert_last_query[[
SELECT 
   f_infoid as id
  FROM T_Info
  info INNER JOIN T_Actor author ON ( info.f_author_id = author.F_ActorID ) INNER JOIN T_Section section ON ( info.f_section_id = section.F_SectionID )  
  WHERE (author.f_name = 'Plutarch' AND section.f_name = 'Ancient Rome') 
]]



require'loft.extract'
local ddl = loft.extract.render{engine=engine, format='physical'}

assert(short_query(ddl)==short_query[[
CREATE TABLE IF NOT EXISTS T_Editor ( 
  f_id BIGINT(8) PRIMARY KEY NOT NULL AUTO_INCREMENT, 
F_ActorID BIGINT(8)
);
CREATE TABLE IF NOT EXISTS T_Actor ( 
  F_ActorID BIGINT(8) PRIMARY KEY NOT NULL AUTO_INCREMENT, 
f_name VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS T_Info ( 
  f_infoid BIGINT(8) PRIMARY KEY NOT NULL AUTO_INCREMENT, 
f_title VARCHAR(100), 
f_summary LONGTEXT, 
f_fulltext LONGTEXT, 
f_section_id BIGINT(8), 
f_authorName VARCHAR(255), 
f_authorMail VARCHAR(255), 
f_author_id BIGINT(8), 
f_creatorActor_id BIGINT(8), 
f_state INT(10) COMMENT  'Info state, can assume one of ''5'' values'
);
CREATE TABLE IF NOT EXISTS T_Section ( 
  F_SectionID BIGINT(8) PRIMARY KEY NOT NULL AUTO_INCREMENT, 
f_editor_id BIGINT(8), 
f_name VARCHAR(100), 
f_tag VARCHAR(100)
);
]])

provider.search(engine, {
	default.entities.info,
	include_fields = {
		'id'
	}, 	
	filters = {
		section_editor_actor_name = 'Fulano de Tal',
	}
}) 
