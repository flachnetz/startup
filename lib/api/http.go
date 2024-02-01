package api

import (
	"net/http"
	"strings"
)

type SiteId string

func (s SiteId) ToLower() SiteId {
	return SiteId(strings.ToLower(string(s)))
}

func (s SiteId) ToUpper() SiteId {
	return SiteId(strings.ToUpper(string(s)))
}

func GetSiteFromHeader(header http.Header) (SiteId, error) {
	siteId := header.Get("Site")
	if siteId == "" {
		return "", ErrSiteMissing
	}

	return SiteId(siteId), nil
}
