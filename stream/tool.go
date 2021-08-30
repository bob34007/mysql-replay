package stream

func IsSelectStmtOrSelectPrepare(query string) bool {
	/*s := strings.ToLower(query)
	s1 := strings.TrimSpace(s)
	return strings.HasPrefix(s1, "select")*/
	if len(query) < 6 {
		return false
	}
	for i, x := range query {
		if x == ' ' {
			continue
		} else {
			if len(query)-i < 6 {
				return false
			} else {
				if (query[i] == 'S' || query[i] == 's') &&
					(query[i+1] == 'E' || query[i+1] == 'e') &&
					(query[i+2] == 'L' || query[i+2] == 'l') &&
					(query[i+3] == 'E' || query[i+3] == 'e') &&
					(query[i+4] == 'C' || query[i+4] == 'c') &&
					(query[i+5] == 'T' || query[i+5] == 't') {
					return true
				}
				return false
			}
		}

	}
	return false
}
