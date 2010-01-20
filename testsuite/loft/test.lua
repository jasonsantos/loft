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
	assert(type(loft.engine())=='table')
end

do -- engine structure is valid
	local L = loft.engine()
	assert(L.new)
	assert(L.save)
	assert(L.get)
	assert(L.destroy)
	assert(L.find)
	assert(L.decorate)
end

do -- engine separation on plugins
	local counter = 0
	loft.plugins.add{
		name = 'engine_setup',
		configure = function(plugin, engine)
			counter = counter + 1
			engine.test = counter
		end,
		run = function(...) end
	}
	local L = loft.engine()
	local M = loft.engine()
	assert(L.test==1)
	assert(M.test==2)
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

do -- test separated plugin configurations for different engines
	loft.plugins.add{
		name = 'isolated_plugin',
		configure = function(plugin, engine)
		end,
		run = function(plugin, engine)
			return plugin.property .. tostring(engine.options.random_property)
		end
	}
	local L = loft.engine()
	local M = loft.engine{ random_property = 'yes' }
	
	local p1 = L.plugins.isolated_plugin{ property='A' }
	local p2 = M.plugins.isolated_plugin{ property='B' }
	
	assert(p1 ~= 2)
	
	assert(p1.property=='A')
	assert(p2.property=='B')
	
	assert(p1.run()=='Anil')
	assert(p2.run()=='Byes')
end

do -- testing the 'new' method on the public API
	local L = loft.engine()
	local o = L.new({'Simple'},{name='Barbara Wright'})
	assert(o.name=='Barbara Wright')
end