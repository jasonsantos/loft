.PHONY : deploy shake-r shake exec

deploy: 
ifeq ($(findstring rockspec,$(wildcard *.rockspec)), )
	cd rockspec; luarocks make
else
	luarocks make
endif

shake-r:
	shake -r
shake:
	cd $(path); shake;
exec:
	lua5.1 -lluarocks.require $(file)