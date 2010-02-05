package.path = ';;../../source/?.lua'

require 'events'

-- Configuration api tests

do -- module existence  
	assert(events)
end

do -- api existence  
	assert(events.watch)
	assert(events.notify)
	assert(events.watchers)
	assert(events.remove)
end

do -- simple use with functions
	local touched
	assert(events.watch('randomEvent', 'randomContext', function(...) touched = true end))
	assert(events.notify('randomEvent', 'randomContext'))
	assert(touched)
end

do -- watcher verification and removal
	assert(events.watchers('anotherRandomEvent', 'randomContext')==0)
	assert(events.watch('anotherRandomEvent', 'randomContext', function(...) noop() end))
	assert(events.watchers('anotherRandomEvent', 'randomContext')==1)
	assert(events.watch('anotherRandomEvent', 'randomContext', function(...) noop2() end))
	assert(events.watchers('anotherRandomEvent', 'randomContext')==2)
	
	events.remove('anotherRandomEvent', 'randomContext')
	assert(events.watchers('anotherRandomEvent', 'randomContext')==0)
end

do -- simple use with a sequence of functions
	local runs = 0
	assert(events.watch('randomEvent', 'anotherRandomContext', {
		(function(...) runs = runs + 1 end),
		(function(...) runs = runs + 1 end),
		(function(...) runs = runs + 1 end),
	}))
	assert(events.notify('randomEvent', 'anotherRandomContext'))
	assert(runs==3)
end

do -- simple use with multiple registrations
	local runs = 0
	assert(events.watch('randomEvent', 'yetAnotherRandomContext', function(...) runs = runs + 1 end))
	assert(events.watch('randomEvent', 'yetAnotherRandomContext', function(...) runs = runs + 1 end))
	
	assert(events.notify('randomEvent', 'yetAnotherRandomContext'))
	
	assert(runs==2)
end

do -- multiple calls
	local runs = 0
	assert(events.watch('yetAnotherRandomEvent', 'randomContext', function(...) runs = runs + 1 end))
	assert(events.watch('yetAnotherRandomEvent', 'randomContext', function(...) runs = runs + 1 end))
	
	assert(events.notify('yetAnotherRandomEvent', 'randomContext'))
	assert(events.notify('yetAnotherRandomEvent', 'randomContext'))
	assert(events.notify('yetAnotherRandomEvent', 'randomContext'))
	
	assert(runs==6)
end

