package api

import "net/http"

type SiteId string

func (s SiteId) ToLower() SiteId {
	return SiteId(s.ToLower())
}

func (s SiteId) ToUpper() SiteId {
	return SiteId(s.ToUpper())
}

func GetSiteFromHeader(header http.Header) (SiteId, error) {
	siteId := header.Get("Site")
	if siteId == "" {
		return "", ErrSiteMissing
	}

	return SiteId(siteId), nil
}
