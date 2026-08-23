package main

import "testing"

func TestClientURIRoundTripMultiBackend(t *testing.T) {
	cfgBackends := []BackendConfig{
		{URL: "http://host1:8081", Login: "user1", Password: "pass1"},
		{URL: "https://host2:8082", Login: "user2", Password: "pass2"},
		{URL: "http://host3", Login: "user3", Password: "pass3"},
	}

	uri := serverClientURI(cfgBackends, "http://host1:8081", "user1", "pass1", true)

	webdavURL, login, password, _, extra := parseClientURI(uri)

	if webdavURL != "http://host1:8081" || login != "user1" || password != "pass1" {
		t.Fatalf("primary backend = (%q, %q, %q), want (http://host1:8081, user1, pass1)", webdavURL, login, password)
	}
	if len(extra) != 2 {
		t.Fatalf("len(extra) = %d, want 2 (extra: %+v)", len(extra), extra)
	}
	if extra[0].URL != "https://host2:8082" || extra[0].Login != "user2" || extra[0].Password != "pass2" {
		t.Errorf("extra[0] = %+v", extra[0])
	}
	if extra[1].URL != "http://host3" || extra[1].Login != "user3" || extra[1].Password != "pass3" {
		t.Errorf("extra[1] = %+v", extra[1])
	}
}

func TestClientURISingleBackendHasNoExtra(t *testing.T) {
	uri := serverClientURI(nil, "https://dav.example.com", "user", "pass", false)

	webdavURL, login, password, _, extra := parseClientURI(uri)

	if webdavURL != "https://dav.example.com" || login != "user" || password != "pass" {
		t.Fatalf("primary backend = (%q, %q, %q)", webdavURL, login, password)
	}
	if len(extra) != 0 {
		t.Fatalf("len(extra) = %d, want 0", len(extra))
	}
}

func TestParseClientURIOldStyleSingleBackendUnaffected(t *testing.T) {
	// A URI with no backend= params (the pre-existing format) must still
	// parse with an empty extra list, so old saved URIs keep working.
	webdavURL, login, password, q, extra := parseClientURI("webdav://user:pass@host:8080?poll-min=50ms")

	if webdavURL != "http://host:8080" || login != "user" || password != "pass" {
		t.Fatalf("got (%q, %q, %q)", webdavURL, login, password)
	}
	if q.Get("poll-min") != "50ms" {
		t.Errorf("poll-min = %q, want 50ms", q.Get("poll-min"))
	}
	if len(extra) != 0 {
		t.Fatalf("len(extra) = %d, want 0", len(extra))
	}
}
