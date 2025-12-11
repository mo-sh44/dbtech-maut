package de.htwberlin.dbtech.aufgaben.ue03.DaoMapper;


import de.htwberlin.dbtech.exceptions.DataException;

import java.sql.Connection;

public class MauterhebungMapper {
    private Connection connection;

    public MauterhebungMapper(Connection connection) {
        this.connection = connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

}
