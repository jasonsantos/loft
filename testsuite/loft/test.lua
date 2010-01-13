package.path = ';;../../source/?.lua'

require 'loft'

-- Configuration api tests

do -- function existence  
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

