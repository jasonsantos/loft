require'luarocks.require'
local lpeg = require'lpeg'

module('stomp', package.seeall)

--- creates a new catalog
function catalog(cat, parentCatalog)
	local parentCatalog = parentCatalog or (type(cat[1])=='table' and cat[1])
	return setmetatable(cat, {
		__index = parentCatalog
	})
end

--- creates a new template
function template(name, tpl, parentTemplate)
	tpl[1] = parentTemplate or (type(tpl[1])=='table' and tpl[1])
	local name = type(name)=='string' and name or 'default'
	local templ = type(name)=='table' and name or tpl
	return catalog{[name]=templ}
end

--- formats a list of values as a dataset
function dataset(list, additionalAttributes) 
	local res = {unpack(list)}
	for k,v in pairs(additionalAttributes) do
		res[k]=v
	end
	return res
end

-- returns a string rendered as a
local function renderTemplate(context)
	local templateString = context.templateString
	local dataset =  context.dataset
end

--- renders a template a number of times equal to the size of the list
-- it can also use subtemplates or 
function render(options)
	local template = options.template
	local dataset = dataset(options.list, options.attributes)
	
	
end

print'123'