dofile('../../lib/util.lua')

package.path=";;../../lib/?.lua"

local stomp = require'stomp'

local render = stomp.render

local r = render{ 
	template={
		[[Listen to your heart]]
	}, 
	value={
		1
	}
}

assert(r=='Listen to your heart')

r = render{ 
	template={
		[[Listen to your heart number ${}]]
	}, 
	value={
		1
	}
}

assert(r=='Listen to your heart number 1')

r = render{ 
	template={
		[[${action} to your heart number ${}]]
	}, 
	value={
		1;
		action='Listen'
	}
}

assert(r=='Listen to your heart number 1')

r = render{ 
	template={
		[[${actions:, } to your heart number ${}]],
	}, 
	value={
		1;
		actions={'Listen', 'Hark', 'Hear'}
	}
}

assert(r=='Listen, Hark, Hear to your heart number 1')

r = render{ 
	template={
		[[${actions:, } to your heart number ${}]];
		actions="'${}'"
	}, 
	value={
		1;
		actions={'Listen', 'Hark', 'Hear'}
	}
}

assert(r=="'Listen', 'Hark', 'Hear' to your heart number 1")

r = render{ 
	template={
		[[${actions:, } to your heart number ${}]];
		actions="${action} ${article} ${object}"
	}, 
	value={
		1;
		actions={
			{
				action='Write',
				article='a',
				object='Book',
			},
			{
				action='Plant',
				article='a',
				object='Tree',
			},
			{
				action='Raise',
				article='several',
				object='Children',
			},
		}
	}
}

assert(r=="Write a Book, Plant a Tree, Raise several Children to your heart number 1")

r = render{ 
	template={
		[[Alternate template with no value]];
	}, 
}

assert(r=='')

r = render{ 
	template={
		[[Blind template with a string value]];
	}, 
	value='This value will not appear'
}

assert(r=='Blind template with a string value')

r = render{ 
	template={
		[[Standard template with a string value and a placeholder '${}']];
	}, 
	value='This value definitely will appear'
}

assert(r=="Standard template with a string value and a placeholder 'This value definitely will appear'")

r = render{ 
	template={
		[[All naipes are: ${naipes:, } and all cards are ${cards:, }]];
		naipes=[['${}']],
		cards=[['${name} of ${naipe}']],
	}, 
	value={0;
		naipes={"Diamonds", "Clubs", "Spades", "Hearts"}, 
		cards={
			{name='Ace', naipe='Spades'}
		}
	}
}

assert(r==[[All naipes are: 'Diamonds', 'Clubs', 'Spades', 'Hearts' and all cards are 'Ace of Spades']])


local testSchema = {0;
  ["tableName"]='T_Info',
  ["columns"]= {
    [1]= {
      ["name"]='id',
      ["required"]= 1,
      ["onEscape"]= 'function: 0x40c330',
      ["type"]='BIGINT',
      ["primary"]= 1,
      ["columnName"]='F_Id',
    },
    [2]= {
      ["name"]='title',
      ["maxlength"]= 250,
      ["size"]= 100,
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='VARCHAR',
      ["columnName"]='F_Title',
    },
    [3]= {
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='LONGTEXT',
      ["name"]='summary',
      ["columnName"]='F_Summary',
    },
    [4]= {
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='LONGTEXT',
      ["name"]='fullText',
      ["columnName"]='F_Fulltext',
    },
    [5]= {
      ["name"]='section',
      ["size"]='8',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='INT',
      ["columnName"]='F_Section',
    },
    [6]= {
      ["name"]='authorName',
      ["size"]='255',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='VARCHAR',
      ["columnName"]='F_Authorname',
    },
    [7]= {
      ["name"]='authorMail',
      ["size"]='255',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='VARCHAR',
      ["columnName"]='F_Authormail',
    },
    ["fullText"]= {
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='LONGTEXT',
      ["name"]='fullText',
      ["columnName"]='F_Fulltext',
    },
    [9]= {
      ["name"]='creatorActor',
      ["size"]='8',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='BIGINT',
      ["columnName"]='F_Creatoractor',
    },
    [10]= {
      ["name"]='state',
      ["onSet"]= 'function: 0x20fda10',
      ["size"]= 10,
      ["onGet"]= 'function: 0x20fff20',
      ["onEscape"]= 'function: 0x40c330',
      ["type"]='INT',
      ["columnName"]='F_State',
    },
    ["creatorActor"]= {
      ["name"]='creatorActor',
      ["size"]='255',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='VARCHAR',
      ["columnName"]='F_Creatoractor',
    },
    ["state"]= {
      ["name"]='state',
      ["onSet"]= 'function: 0x20fda10',
      ["size"]= 10,
      ["onGet"]= 'function: 0x20fff20',
      ["onEscape"]= 'function: 0x40c330',
      ["type"]='INT',
      ["columnName"]='F_State',
    },
    [8]= {
      ["name"]='actor',
      ["size"]='8',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='BIGINT',
      ["columnName"]='F_Actor',
    },
    ["n"]= 10,
    ["authorMail"]= {
      ["name"]='authorMail',
      ["size"]='255',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='VARCHAR',
      ["columnName"]='F_Authormail',
    },
    ["authorName"]= {
      ["name"]='authorName',
      ["size"]='255',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='VARCHAR',
      ["columnName"]='F_Authorname',
    },
    ["actor"]= {
      ["name"]='actor',
      ["size"]='8',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='BIGINT',
      ["columnName"]='F_Actor',
    },
    ["title"]= {
      ["name"]='title',
      ["maxlength"]= 250,
      ["size"]= 100,
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='VARCHAR',
      ["columnName"]='F_Title',
    },
    ["summary"]= {
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='LONGTEXT',
      ["name"]='summary',
      ["columnName"]='F_Summary',
    },
    ["section"]= {
      ["name"]='section',
      ["size"]='8',
      ["onEscape"]= 'function: 0x2118d30',
      ["type"]='BIGINT',
      ["columnName"]='F_Section',
    },
    ["id"]= {
      ["name"]='id',
      ["required"]= 1,
      ["onEscape"]= 'function: 0x40c330',
      ["type"]='BIGINT',
      ["primary"]= 1,
      ["columnName"]='F_Id',
    },
  },
 }

r = render{ 
	template={
		[[SELECT ${columns:,} FROM ${tableName}]];
		columns=[[${columnName}]],
	}, 
	value = testSchema
}

assert(r=='SELECT F_Id,F_Title,F_Summary,F_Fulltext,F_Section,F_Authorname,F_Authormail,F_Actor,F_Creatoractor,F_State FROM T_Info')

r = render{ 
	template={
		[[SELECT ${columns:, } FROM ${tableName}]];
		columns=[[${columnName} as ${name}]],
	}, 
	value = testSchema
}

assert(r=='SELECT F_Id as id, F_Title as title, F_Summary as summary, F_Fulltext as fullText, F_Section as section, F_Authorname as authorName, F_Authormail as authorMail, F_Actor as actor, F_Creatoractor as creatorActor, F_State as state FROM T_Info')

local testData = {0;
	columns=testSchema.columns,
	tableName=testSchema.tableName,
	data={
		{1, 'Matéria 1', 'Sumário da Matéria 1', 'Fulltext da Matéria', 1, 'Jason', 'jasonsantos@fabricadigital.com.br', 6, 6, 5}
	}
}



r = render{ 
	template={
		[[INSERT INTO TABLE ${tableName} (${columns:, }) VALUES (${data:, })]];
		columns=[[${columnName}]],
		data=function(t, ctx)
			local t = t or {}
			local sep = ctx.separator or ''
			local res = {}
			local idx = 1

			while idx<=getn(t) do
				local field = ctx.value.columns[idx]
				local value
				if field.type=='LONGTEXT' or field.type=='VARCHAR' then
					value = "'"..tostring(t[idx]).."'"
				else
					value = t[idx]
				end
				tinsert(res, value)
				idx=idx+1
			end
			return tconcat(res, sep)
		end
	}, 
	value = testData
}


assert(r=="INSERT INTO TABLE T_Info (F_Id, F_Title, F_Summary, F_Fulltext, F_Section, F_Authorname, F_Authormail, F_Actor, F_Creatoractor, F_State) VALUES (1, 'Matéria 1', 'Sumário da Matéria 1', 'Fulltext da Matéria', 1, 'Jason', 'jasonsantos@fabricadigital.com.br', 6, 6, 5)")



local testData = {0;
	columns=testSchema.columns,
	tableName=testSchema.tableName,
}


-- Funções úteis ao renderer
-- -------------------------

local Constants = {
	REQUIRED = "NOT NULL",
	PRIMARY_KEY = "PRIMARY KEY",
	KEY = "KEY",
	COMMENT = "COMMENT"
}


local ColumnType=function(column)
	if column.type then
		return ' ' .. column.type .. ' '
	end	
	error('Column "' ..  tostring(column.name) .. '" does not have a proper type')
end

-- for columns on Create Table
local ColumnSize=function(column)
	local columnSize = column.maxlength or column.size
	if columnSize then
		if type(columnSize)=='table' then
			columnSize = tconcat(columnSize, ', ')
		end
		
		return ' (' .. tostring(columnSize) .. ') '
	else
		return ' '
	end
end

local ColumnRequired=function(column)
	if column.required == true then
		return ' ' .. %Constants.REQUIRED .. ' '
	else
		return ''
	end
end

local ColumnIsKey=function(column)
	if column.primary == true then
		return ' ' .. %Constants.PRIMARY_KEY .. ' '
	elseif column.key == true then
		return ' ' .. %Constants.KEY .. ' '
	else 
		return ''
	end
end

local ColumnComments=function(column)
	if column.description then
		return ' ' .. %Constants.COMMENT .. %stringLiteral(column.description) .. ' '
	else 
		return ''
	end
end

r = render{ 
	template={
		[[CREATE TABLE ${tableName} IF NOT EXISTS ( ${createColumns:, } )]];
		createColumns=function(_, ctx)
			local lines = {}
			local cols = ctx.value.columns
			local i = 1
			local n = getn(cols)
			while i<=n do
				local col = cols[i]
				local columnLine = col.columnName 
						.. %ColumnType(col)
						.. %ColumnSize(col) 
						.. %ColumnRequired(col) 
						.. %ColumnIsKey(col) 
						.. %ColumnComments(col)
					
				tinsert(lines, columnLine)
				i=i+1
			end 
			return tconcat(lines, ctx.separator or '')
		end,
	},
	value = testData
}

assert(r=='CREATE TABLE T_Info IF NOT EXISTS ( F_Id BIGINT   NOT NULL  PRIMARY KEY , F_Title VARCHAR  (250) , F_Summary LONGTEXT  , F_Fulltext LONGTEXT  , F_Section INT  (8) , F_Authorname VARCHAR  (255) , F_Authormail VARCHAR  (255) , F_Actor BIGINT  (8) , F_Creatoractor BIGINT  (8) , F_State INT  (10)  )')

local T = this.Template{
	default = {
		CREATE = {
			[[CREATE TABLE ${tableName} IF NOT EXISTS ( ${columns:, } )]];
			columns=[[${columnName} ${type}${size}]],
			size=[[(${})]]
		},
	},
	mysql = {
		'default';
	},
}


tinfo = {
	[1]= {
	  name='id',
	  primary= 1,
	  type='BIGINT',
	  columnName='F_Id',

	  required= 1,
	},
	[2]= {
	  name='title',
	  size= 100,
	  maxlength= 250,
	  columnName='F_Title',
	  type='VARCHAR',

	},
	[3]= {

	  name='summary',
	  columnName='F_Summary',
	  type='LONGTEXT',
	},
	[4]= {

	  name='fullText',
	  columnName='F_Fulltext',
	  type='LONGTEXT',
	},
	[5]= {
	  size='255',
	  name='section',
	  columnName='F_Section',
	  type='VARCHAR',

	},
	[6]= {
	  size='255',
	  name='authorName',
	  columnName='F_Authorname',
	  type='VARCHAR',

	},
	[7]= {
	  size='255',
	  name='authorMail',
	  columnName='F_Authormail',
	  type='VARCHAR',

	},
	[8]= {
	  size='255',
	  name='actor',
	  columnName='F_Actor',
	  type='VARCHAR',

	},
	[9]= {
	  size='255',
	  name='creatorActor',
	  columnName='F_Creatoractor',
	  type='VARCHAR',

	},
	[10]= {
	  name='state',

	  size= 10,
	  columnName='F_State',
	  type='INT',


	},
	summary= {

	  name='summary',
	  columnName='F_Summary',
	  type='LONGTEXT',
	},
	authorName= {
	  size='255',
	  name='authorName',
	  columnName='F_Authorname',
	  type='VARCHAR',

	},
	authorMail= {
	  size='255',
	  name='authorMail',
	  columnName='F_Authormail',
	  type='VARCHAR',

	},
	actor= {
	  size='255',
	  name='actor',
	  columnName='F_Actor',
	  type='VARCHAR',

	},
	creatorActor= {
	  size='255',
	  name='creatorActor',
	  columnName='F_Creatoractor',
	  type='VARCHAR',

	},
	state= {
	  name='state',

	  size= 10,
	  columnName='F_State',
	  type='INT',


	},
	fullText= {

	  name='fullText',
	  columnName='F_Fulltext',
	  type='LONGTEXT',
	},
	id= {
	  name='id',
	  primary= 1,
	  type='BIGINT',
	  columnName='F_Id',

	  required= 1,
	},
	section= {
	  size='255',
	  name='section',
	  columnName='F_Section',
	  type='VARCHAR',

	},
	title= {
	  name='title',
	  size= 100,
	  maxlength= 250,
	  columnName='F_Title',
	  type='VARCHAR',

	}
}

local Q = T.mysql

local sql = Q.CREATE < {0;
  tableName='T_Info',
  columns = tinfo
}

assert(sql==[[CREATE TABLE T_Info IF NOT EXISTS ( F_Id BIGINT, F_Title VARCHAR(100), F_Summary LONGTEXT, F_Fulltext LONGTEXT, F_Section VARCHAR(255), F_Authorname VARCHAR(255), F_Authormail VARCHAR(255), F_Actor VARCHAR(255), F_Creatoractor VARCHAR(255), F_State INT(10) )]])


local info = {
  ["tableName"]='T_Info',
  ["columns"]= {
    ["title"]= {
      ["type"]='VARCHAR',
      ["maxlength"]= 250,
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='title',
      ["columnName"]='F_Title',
      ["size"]= 100,
    },
    [1]= {
      ["type"]='BIGINT',
      ["columnName"]='F_Id',
      ["onEscape"]= 'function: 0x804b43f',
      ["name"]='id',
      ["required"]= 1,
      ["size"]='8',
      ["primary"]= 1,
    },
    [2]= {
      ["type"]='VARCHAR',
      ["maxlength"]= 250,
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='title',
      ["columnName"]='F_Title',
      ["size"]= 100,
    },
    [3]= {
      ["type"]='LONGTEXT',
      ["name"]='summary',
      ["columnName"]='F_Summary',
      ["onEscape"]= 'function: 0x812dc60',
    },
    [4]= {
      ["type"]='LONGTEXT',
      ["name"]='fullText',
      ["columnName"]='F_Fulltext',
      ["onEscape"]= 'function: 0x812dc60',
    },
    [5]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='section',
      ["columnName"]='F_Section',
      ["size"]='255',
    },
    [6]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='authorName',
      ["columnName"]='F_Authorname',
      ["size"]='255',
    },
    [7]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='authorMail',
      ["columnName"]='F_Authormail',
      ["size"]='255',
    },
    ["id"]= {
      ["type"]='BIGINT',
      ["columnName"]='F_Id',
      ["onEscape"]= 'function: 0x804b43f',
      ["name"]='id',
      ["required"]= 1,
      ["size"]='8',
      ["primary"]= 1,
    },
    [9]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='creatorActor',
      ["columnName"]='F_Creatoractor',
      ["size"]='255',
    },
    [10]= {
      ["type"]='INT',
      ["onEscape"]= 'function: 0x804b43f',
      ["name"]='state',
      ["columnName"]='F_State',
      ["onSet"]= 'function: 0x813eae0',
      ["description"]="Estado da info, pode assumir um de '5' valores",
      ["onGet"]= 'function: 0x813eb60',
      ["size"]= 10,
    },
    ["n"]= 10,
    ["state"]= {
      ["type"]='INT',
      ["onEscape"]= 'function: 0x804b43f',
      ["name"]='state',
      ["columnName"]='F_State',
      ["onSet"]= 'function: 0x813eae0',
      ["description"]="Estado da info, pode assumir um de '5' valores",
      ["onGet"]= 'function: 0x813eb60',
      ["size"]= 10,
    },
    ["summary"]= {
      ["type"]='LONGTEXT',
      ["name"]='summary',
      ["columnName"]='F_Summary',
      ["onEscape"]= 'function: 0x812dc60',
    },
    ["fullText"]= {
      ["type"]='LONGTEXT',
      ["name"]='fullText',
      ["columnName"]='F_Fulltext',
      ["onEscape"]= 'function: 0x812dc60',
    },
    ["creatorActor"]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='creatorActor',
      ["columnName"]='F_Creatoractor',
      ["size"]='255',
    },
    [8]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='actor',
      ["columnName"]='F_Actor',
      ["size"]='255',
    },
    ["authorName"]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='authorName',
      ["columnName"]='F_Authorname',
      ["size"]='255',
    },
    ["section"]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='section',
      ["columnName"]='F_Section',
      ["size"]='255',
    },
    ["authorMail"]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='authorMail',
      ["columnName"]='F_Authormail',
      ["size"]='255',
    },
    ["actor"]= {
      ["type"]='VARCHAR',
      ["onEscape"]= 'function: 0x812dc60',
      ["name"]='actor',
      ["columnName"]='F_Actor',
      ["size"]='255',
    },
  },
}

local stringLiteral=function(s) 
	return "'" .. gsub(s or '', "'", "''") .. "'" 
end

local T = stomp{
		CREATE = {
			[[CREATE TABLE ${tableName} IF NOT EXISTS ( ${columns:, } )]];
			columns=[[${columnName} ${type}${size}${primary}${required}${description}]],
			size=[[(${})]],
			primary=[[ PRIMARY KEY]],
			required=[[ NOT NULL]],
			description={[[ COMMENT ${}]], stringLiteral},
		},
	}

local sql = T.CREATE < merge({true},info)


assert(sql==[[CREATE TABLE T_Info IF NOT EXISTS ( F_Id BIGINT(8) PRIMARY KEY NOT NULL, F_Title VARCHAR(100), F_Summary LONGTEXT, F_Fulltext LONGTEXT, F_Section VARCHAR(255), F_Authorname VARCHAR(255), F_Authormail VARCHAR(255), F_Actor VARCHAR(255), F_Creatoractor VARCHAR(255), F_State INT(10) COMMENT 'Estado da info, pode assumir um de ''5'' valores' )]])


local t = stomp[[este é um template número ${}]]

assert((t<{1})=='este é um template número 1')


local t = stomp{
	THIS={
		[[este é um template do ${pai}]];
		pai=[[mostra ${} ${pai}]]
	}
}

local r = (t.THIS<{true;
	pai={1;
		pai={2;
			pai={3;
				pai={4;
				}
			}
		}
	}
})

assert(r=='este é um template do mostra 1 mostra 2 mostra 3 mostra 4 ')


print'<<OK>>'
