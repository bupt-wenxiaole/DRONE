package Set

import (
	"graph"
	"fmt"
)

type Set map[graph.ID]bool

func (s Set) Add(id graph.ID) {
	if s == nil {
		fmt.Println("zs-log: fuck")
	}
	s[id] = true
}

func NewSet() Set {
	return make(map[graph.ID]bool)
}

func (s Set) Copy() Set {
	tmp := NewSet()
	for v := range s {
		tmp.Add(v)
	}
	return tmp
}

func (s Set) Size() int {
	return len(s)
}

func (s Set) Merge(an Set) {
	for v := range an {
		s[v] = true
	}
}

func (s Set) Separate(an Set)  {
	for v := range an {
		delete(s, v)
	}
}

func (s Set) Has(v graph.ID) bool {
	_, ok := s[v]
	return ok
}

func (s Set) Remove(v graph.ID)  {
	delete(s, v)
}

func (s Set) HasIntersection(an Set) bool {
	var a, b Set
	if s.Size() < an.Size() {
		a = s
		b = an
	} else {
		a = an
		b = s
	}

	for v := range a {
		if b.Has(v) {
			return true
		}
	}
	return false
}


func GetPreSet(g graph.Graph, id graph.ID) Set {
	preSet := NewSet()
	sources, _ := g.GetSources(id)
	for sid := range sources {
		preSet.Add(sid)
	}
	for _, msg := range g.GetFOs()[id] {
		preSet.Add(msg.RelatedId())
	}
	return preSet
}

func GetPostSet(g graph.Graph, id graph.ID) Set {
	postSet := NewSet()
	targets, _ := g.GetTargets(id)
	for sid := range targets {
		postSet.Add(sid)
	}
	for _, msg := range g.GetFIs()[id] {
		postSet.Add(msg.RelatedId())
	}
	return postSet
}
