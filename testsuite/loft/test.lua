package.path = [[;;./net.luaforge.loft/source/lua/5.1/?.lua;./net.luaforge.loft/source/lua/5.1/?/init.lua;]]

require"loft"

media = {};

-- Mock persistence provider
package.loaded['loft.providers.mock'] = {
	
	supportSchemas=function() 
		return true 
	end;

	getNextId=(function()
		local id = 1
		return function(...) 
			print('getNextId', ...)
			id = id + 1
			return id 
		end 
	end)();

	persist=function(class, id, obj) 
		media[class] = media[class] or {} 
		media[class][id] = obj
		print('persist',id)
		return true 
	end;
	
	retrieve=function(class, id) 
		print('retrieve',id)
		media[class] = media[class] or {}
		return media[class][id] 
	end;

	persistSimple=function(id, obj) 
		print('persistSimple', id)

		media['__simple__'] = media['__simple__'] or {} 
		media[class][id] = obj
	end;
	
	retrieveSimple=function(id) 
		print('retrieveSimple', id)
		media['__simple__'] = media['__simple__'] or {} 
		media[class][id] = obj
	end;

	search=function(...) 
		print('search', ...)
		return true 
	end;
}
 

local loft = loft.initialize('mock')

assert(loft.getProvider==nil) -- mustn't see the provider

-- registering a valid schema
loft.registerSchema('presente', {
	nome='';
	descricao='';
	url='http://amazon.com';
	quantidadeDesejada=1;
	quantidadeReservada=0;
	quantidadeComprada=0;
})

-- registering an invalid schema
local err = pcall(loft.registerSchema, 'convidado', 'valor invalido')
assert(not err)

-- testing new() case 1/3
local presente = loft.new'presente'

-- testing metatable
assert(presente.url=='http://amazon.com')
assert(presente.quantidadeDesejada==1)
assert(presente.quantidadeReservada==0)
assert(presente.quantidadeComprada==0)

presente.quantidadeReservada = 1

assert(presente.quantidadeReservada==1)
assert(presente.quantidadeComprada==0)

assert(presente.id==2)

presente.id = 4 -- metatable prevents changing ID

assert(presente.id==2)

-- testing new() case 2/3
local presente2 = loft.new('presente',{
	id = 102;
	nome='Luís Eduardo Jason Santos';
	descricao='Developer';
	url='http://quantumsatis.net';
	quantidadeDesejada=5;
	quantidadeReservada=0;
	quantidadeComprada=0;
	outraQuantidade=1;
})

assert(presente2.url=='http://quantumsatis.net')
assert(presente2.nome=='Luís Eduardo Jason Santos')
assert(presente2.quantidadeDesejada==5)
assert(presente2.outraQuantidade==1)

assert(presente2.id==102)

-- testing new() case 3/3
local presente3 = loft.new('presente',{
	nome='Luís Eduardo Jason Santos';
	descricao='Developer';
	url='http://quantumsatis.net';
	quantidadeDesejada=5;
	quantidadeReservada=0;
	quantidadeComprada=0;
	outraQuantidade=1;
}, 103)

assert(presente3.url=='http://quantumsatis.net')
assert(presente3.nome=='Luís Eduardo Jason Santos')
assert(presente3.quantidadeDesejada==5)
assert(presente3.outraQuantidade==1)

assert(presente3.id==103)

loft.save(presente)
loft.save(presente2)
loft.save(presente3)

-- testing time-based pool 

local altPresente = loft.findById(2, 'presente')
local altPresente2 = loft.findById(102, 'presente')
local altPresente3 = loft.findById(103, 'presente')

-- these objects were not changed this time

assert(presente == altPresente)
assert(presente2 == altPresente2)
assert(presente3 == altPresente3)

presente = nil
presente2 = nil
presente3 = nil
altPresente = nil
altPresente2 = nil
altPresente3 = nil

local altPresente = loft.findById(2, 'presente')
local altPresente2 = loft.findById(102, 'presente')
local altPresente3 = loft.findById(103, 'presente')

assert(altPresente.quantidadeReservada==1)
assert(altPresente.quantidadeComprada==0)

assert(altPresente2.url=='http://quantumsatis.net')
assert(altPresente2.nome=='Luís Eduardo Jason Santos')
assert(altPresente2.quantidadeDesejada==5)
assert(altPresente2.outraQuantidade==1)

assert(altPresente3.url=='http://quantumsatis.net')
assert(altPresente3.nome=='Luís Eduardo Jason Santos')
assert(altPresente3.quantidadeDesejada==5)
assert(altPresente3.outraQuantidade==1)

-- testing simple persistence

local simpleTable = loft.new()

simpleTable.name = 'simple free object'
simpleTable.size = 10

local simpleTableId = simpleTable.id 

loft.save(simpleTable)

local anotherSimpleTable = loft.findById(simpleTableId)

assert(anotherSimpleTable == simpleTable)