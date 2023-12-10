package org.conduktor.demos;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WikimediaChangeSchema {

    @JsonProperty("id")
    private long id;
    @JsonProperty("type")
    private String type;
    @JsonProperty("user")
    private String user;


    WikimediaChangeSchema(long id, String type, String user) {
        this.id = id;
        this.type = type;
        this.user = user;

    }

    @Override
    public String toString() {
        return "{" +
                "id=" + id +
                ", type='" + type + '\'' +
                ", user='" + user + '\'' +
                '}';
    }
    public long getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getUser() {
        return user;
    }

}
