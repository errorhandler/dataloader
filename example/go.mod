module github.com/errorhandler/dataloader/example

go 1.18

replace github.com/errorhandler/dataloader => ../

require (
	github.com/errorhandler/dataloader v0.0.0-00010101000000-000000000000
	github.com/hashicorp/golang-lru v0.5.4
	github.com/patrickmn/go-cache v2.1.0+incompatible
)

require github.com/opentracing/opentracing-go v1.2.0 // indirect
