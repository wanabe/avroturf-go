package avroturf

type Messaging struct {
	NameSpace   string
	SchemaStore *SchemaStore
	Registry    *CachedConfluentSchemaRegistry
}

func NewMessaging(n string, p string, u string) *Messaging {
	return &Messaging{
		NameSpace: n,
		SchemaStore: &SchemaStore{
			Path: p,
		},
		Registry: &CachedConfluentSchemaRegistry{
			Upstream: &ConfluentSchemaRegistry{
				RegistryURL: u,
			},
		},
	}
}
