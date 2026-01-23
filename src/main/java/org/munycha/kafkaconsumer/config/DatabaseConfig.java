package org.munycha.kafkaconsumer.config;

public class DatabaseConfig {

    private String url;
    private String user;
    private String password;
    private TableConfig tables;

    public DatabaseConfig() {
    }

    public DatabaseConfig(String url, String user, String password, TableConfig tables) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.tables = tables;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public TableConfig getTables() {
        return tables;
    }

    public void setTables(TableConfig tables) {
        this.tables = tables;
    }
}
