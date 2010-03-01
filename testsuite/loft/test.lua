package.path = ';;../../source/?.lua'

require 'loft'

-- Configuration api tests

do -- configure function existence  
	assert(loft.configure)
end

do -- simple empty call  
	local o = loft.configure()
	assert(o)
end

do -- whether functions make into the internal configuration  
	local o = {a=1}
	local o2 = loft.configure(o)
	assert(o2.a==1)
end

do -- whether the option argument is not used as reference
	local o = {}
	local o2 = loft.configure(o)
	o2.a = 1
	assert(not o.a)
end

do -- whether changing the defaults reference does not change internal default values
	local o = {a=1}
	local o2 = loft.configure(o)
	o2.a = 2
	local o3 = loft.configure{}
	assert(o3.a == 1)
end

-- Plugin API tests

do -- plugins property existence 
	assert(loft.plugins)
end

do -- plugins API add function existence 
	assert(loft.plugins.add)
end

do -- empty plugins iteration test
	local i=0
	for k,plugin in pairs(loft.plugins) do
		i = i + 1
	end
	assert(i==0)
end

do -- plugins API add function simple test
	loft.plugins.add{
		name='testplugin',
		configure = function(plugin, engine)
		end,
		run = function(plugin, engine)
		end
	} 
	assert(loft.plugins.testplugin)
	assert(loft.plugins.testplugin.name=='testplugin')
	assert(loft.plugins.testplugin.run)
	assert(loft.plugins.testplugin.configure)
end

do -- non-empty plugins iteration test
	local i=0
	for k,plugin in pairs(loft.plugins) do
		i = i + 1
	end
	assert(i==1)
end

do -- existing plugin add function test
	loft.plugins.add{
		name='duplicate plugin',
		configure = function(plugin, engine)
		end,
		run = function(plugin, engine)
		end
	} 
	assert(loft.plugins['duplicate plugin'])
	assert(not pcall(loft.plugins.add, {
		name='duplicate plugin',
		configure = function(plugin, engine)
		end,
		run = function(plugin, engine)
		end
	}))
end


-- Engine API tests

do -- engine function existence
	assert(loft.engine)
end

do -- simple call return
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end
	}
	assert(type(loft.engine{provider='mock'})=='table')
end

do -- engine structure is valid
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end
	}
	local L = loft.engine{provider='mock'}
	assert(L.new)
	assert(L.save)
	assert(L.get)
	assert(L.destroy)
	assert(L.find)
	assert(L.decorate)
end

do -- engine separation on plugins
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end
	}

	local counter = 0
	loft.plugins.add{
		name = 'engine_setup',
		configure = function(plugin, engine)
			counter = counter + 1
			engine.test = counter
		end,
		run = function(...)
		end
	}
	local L = loft.engine{provider='mock'}
	local M = loft.engine{provider='mock'}
	assert(counter==2)
end

do -- test of provider loading throu require
	local touched = 0
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
			touched = 1
		end
	}
	
	local _ = loft.engine{provider='mock'}
	assert(touched==1)
end

--[[
do -- test separated plugin configurations for different engines
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end
	}

	loft.plugins.add{
		name = 'isolated_plugin',
		configure = function(plugin, engine)
		end,
		run = function(plugin, engine)
			return plugin.property .. tostring(engine.options.random_property)
		end
	}
	local L = loft.engine{provider='mock'}
	local M = loft.engine{provider='mock', random_property = 'yes'}
	
	local p1 = L.plugins.isolated_plugin{ property='A' }
	local p2 = M.plugins.isolated_plugin{ property='B' }
	
	assert(p1 ~= 2)
	
	assert(p1.property=='A')
	assert(p2.property=='B')
	
	assert(p1.run()=='Anil')
	assert(p2.run()=='Byes')
end
]]

do -- testing the 'new' method on the public API
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end
	}
	local L = loft.engine{provider='mock'}
	local o = L.new({'Simple'},{name='Barbara Wright'})
	assert(o.name=='Barbara Wright')
end

do -- testing the 'get' method on the public API
	local Person = {'Simple'}
	local L = loft.engine{provider='mock'}
	local o = L.new(Person,{id=1, name='Susan Foreman'})
	assert(o.name=='Susan Foreman')
	local o1=L.get(Person, 1)
	assert(o1.name=='Susan Foreman')
end

do -- testing the 'save' method on the public API
	local name
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end,
		persist=function(e,en,id,data)
			name = data.name
			return true
		end
	}
	
	local L = loft.engine{provider='mock'}
	local o = L.new({'Person'},{id=1, name='Susan'})
	assert(o.name=='Susan')
	o.name = 'Susan Foreman'
	L.save({'Person'},o)
	assert(name=='Susan Foreman')
end

do -- testing the 'save' method with id updating on proxies
	local name
	local savedId = 1
	local inc = function() savedId = savedId + 1; return savedId end
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end,
		persist=function(e,en,id,data)
			name = data.name
			data.id = type(id)~='table' and id or inc()
			return true
		end
	}
	
	local L = loft.engine{provider='mock'}
	local o = L.new({'Person'},{ name='Martha'})
	assert(o.name == 'Martha')
	o.name = 'Martha Jones'
	L.save({'Person'},o)
	assert(name =='Martha Jones')
	assert(o.id == savedId)
	local p = L.new({'Person'},{ id=33, name='Rose Tyler'})
	proxy.touch(p)
	L.save({'Person'},p)
	assert(name =='Rose Tyler')
	assert(p.id == 33)
end


do -- testing the 'destroy' method on the public API
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end,
		persist=function(e,en,id,data)
			-- yea, persisting
			return true
		end,
		delete=function(e,en,id)
			-- oh, erasing alright
			return true
		end
	}
	local L = loft.engine{provider='mock'}
	local o = L.new({'Person'},{id=1, name='Jack Harkness'})
	proxy.touch(o)
	L.save({'Person'},o)
	
	assert(o.name=='Jack Harkness')
	
	assert(L.destroy({'Person'},o))
	
	assert(not o.name)
end

do -- testing the find method on the public API
	local result = {
				{id=1, name='Rose Tyler'},
				{id=2, name='Martha Jones'},
			};
			
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end,
		search=function(e,options)
			local fn = options.visitor
			for _,value in ipairs(result) do
				fn(value)
			end
			return true
		end
	}
	
	local L = loft.engine{provider='mock'}
	
	local l = L.find({'Person'}, {})
	for idx,o in ipairs(l) do
		assert(o.name==result[idx].name)
	end
	-- using the list iterator
	local idx = 1
	for o in l do
		assert(o.name==result[idx].name)
		idx = idx + 1
	end
end


do -- testing the count method on the public API
	local result = {
				{id=1, name='Rose Tyler'},
				{id=2, name='Martha Jones'},
			};
			
	package.loaded['loft.providers.mock'] = {
		setup=function(engine)
		end,
		search=function(e,options)
			local fn = options.visitor
			for _,value in ipairs(result) do
				fn(value)
			end
			return true
		end,
		count=function(e,options)
			return #result
		end
	}
	
	local L = loft.engine{provider='mock'}
	
	local l = L.find({'Person'}, {})
	local n = L.count({'Person'}, {})
	
	assert(n==2)
	assert(n==#l)
end

print'OK'