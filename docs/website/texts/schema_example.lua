
local t = schema.types

default = schema {
  table_names = "t_$entity",
  default_type = "text",
  column_names = "f_$field",
  info = entity {
    fields = {
      id = t.key{ 1 },
      title = t.text{ 2, size = 250 },
      summary = t.long_text{ 3 },
      full_text = t.long_text{ 4 },
      section = t.belongs_to{ 5, "section" },
      author_name = t.text { 6 },
      author_mail = t.text { 7 },
      actor = t.has_one{ 8, "actor" },
      creator_actor = t.has_one { 9, "actor" },
      state = t.integer{ 10, size = 10, get = function (f, v) print 'Getting'; return v end  },
    },
    before_save = function(...) return ...  end
  }
}
