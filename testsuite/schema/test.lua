package.path = ';;../../source/?.lua'

local schema = require "schema"

local function empty_table(t)
  return not next(t)
end

do
  local s = schema.expand(function () end)
  assert(empty_table(s.entities))
end

do
  local s = schema.expand(function () end)
  assert(not s.key)
end

do
  table_prefix = nil
  local s = schema.expand(function ()
			    table_prefix = "t_"
			  end)
  assert(s.table_prefix == "t_")
  assert(not table_prefix)
end

do
  column_prefix = nil
  local s = schema.expand(function ()
			    column_prefix = "f_"
			  end)
  assert(s.column_prefix == "f_")
  assert(not column_prefix)
end

do
  assert(not pcall(schema.expand, function ()
				    info = entity{}
				  end))
end

do
  s = schema.expand(function ()
		      info = entity { fields = { id = key() } }
		    end)
  assert(s.entities.info.table_name == "info")
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.id.order == 1)
  assert(s.entities.info.fields.id.column_name == "id")
end

do
  s = schema.expand(function ()
		      info = entity { table_name = "INFO", fields = { id = key{ column_name = "ID" } } }
		    end)
  assert(s.entities.info.table_name == "INFO")
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.id.order == 1)
  assert(s.entities.info.fields.id.column_name == "ID")
end

do
  s = schema.expand(function ()
		      table_prefix = "t_"
		      column_prefix = "f_"
		      info = entity { fields = { id = key() } }
		    end)
  assert(s.entities.info.table_name == "t_info")
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.id.order == 1)
  assert(s.entities.info.fields.id.column_name == "f_id")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { id = key{ foo = 5 } } }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.id.foo == 5)
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					title = text()
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.id.order == 1)
  assert(s.entities.info.fields.title.type == "text")
  assert(s.entities.info.fields.title.order == 2)
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					title = text{ 250 }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.title.type == "text")
  assert(s.entities.info.fields.title.size == 250)
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					title = long_text()
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.title.type == "long_text")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					title = long_text{ 250 }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.title.type == "long_text")
  assert(s.entities.info.fields.title.size == 250)
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					title = text()
				      }
				    }
		      section = entity { fields = { 
					   id = key(),
					   title = long_text()
					 }
				       }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.title.type == "text")
  assert(s.entities.section.fields.id.type == "key")
  assert(s.entities.section.fields.title.type == "long_text")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					published = boolean()
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.published.type == "boolean")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					published = date()
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.published.type == "date")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					published = timestamp()
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.published.type == "timestamp")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					n_comments = integer()
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.n_comments.type == "integer")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					n_comments = integer{ 10 }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.n_comments.type == "integer")
  assert(s.entities.info.fields.n_comments.size == 10)
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					n_comments = number()
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.n_comments.type == "number")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					tag = taxonomy{ "tags" }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.tag.type == "taxonomy")
  assert(s.entities.info.fields.tag.params[1] == "tags")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					published = timestamp(),
					section = belongs_to{ "section" }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.published.type == "timestamp")
  assert(s.entities.info.fields.section.type == "belongs_to")
  assert(s.entities.info.fields.section.order == 3)
  assert(s.entities.info.fields.section.entity == "section")
  assert(s.entities.info.fields.section.column_name == "section_id")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					published = timestamp(),
					section = has_one{ "section" }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.published.type == "timestamp")
  assert(s.entities.info.fields.section.type == "has_one")
  assert(s.entities.info.fields.section.order == 3)
  assert(s.entities.info.fields.section.entity == "section")
  assert(s.entities.info.fields.section.column_name == "section_id")
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					published = timestamp(),
					section = has_many{ "section" }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.published.type == "timestamp")
  assert(s.entities.info.fields.section.type == "has_many")
  assert(s.entities.info.fields.section.order == 3)
  assert(s.entities.info.fields.section.entity == "section")
  assert(not s.entities.info.fields.section.column_name)
end

do
  s = schema.expand(function ()
		      info = entity { fields = { 
					id = key(),
					published = timestamp(),
					section = has_and_belongs{ "section" }
				      }
				    }
		    end)
  assert(s.entities.info.fields.id.type == "key")
  assert(s.entities.info.fields.published.type == "timestamp")
  assert(s.entities.info.fields.section.type == "has_and_belongs")
  assert(s.entities.info.fields.section.order == 3)
  assert(s.entities.info.fields.section.entity == "section")
  assert(s.entities.info.fields.section.join_table == "info_section")
  assert(not s.entities.info.fields.section.column_name)
end

do
  s = schema.expand(function ()
		      table_prefix = "t_"
		      info = entity { fields = { 
					id = key(),
					published = timestamp(),
					section = has_and_belongs{ "section" }
				      }
				    }
		    end)
  assert(s.entities.info.fields.section.join_table == "t_info_section")
end

do
  s = schema.expand(function ()
		      table_prefix = "t_"
		      section = entity { fields = { 
					id = key(),
					published = timestamp(),
					post = has_and_belongs{ "info" }
				      }
				    }
		    end)
  assert(s.entities.section.fields.post.join_table == "t_post_section")
end

do
  s = schema.expand(function ()
		      table_prefix = "t_"
		      section = entity { 
			aspects = { "workflow" },
			fields = {
			  id = key(),
			  published = timestamp(),
			  post = has_and_belongs{ "info" }
			}
		      }
		    end)
  assert(s.entities.section.aspects[1] == "workflow")
end

do
  s = schema.expand(function ()
		      table_prefix = "t_"
		      section = node { 
			aspects = { "workflow" },
			fields = {
			  id = key(),
			  published = timestamp(),
			  post = has_and_belongs{ "info" }
			}
		      }
		    end, { node = function (t)
				    t.fields.content = { order = 999, type = "text" }
				    return t
				  end })
  assert(s.entities.section.fields.content.type == "text")
end

