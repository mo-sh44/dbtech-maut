package de.htwberlin.dbtech.aufgaben.ue03.TableObjects;

public class Buchungstatus {
    private int B_ID;
    private String STATUS;

    public Buchungstatus(int b_ID, String STATUS) {
        B_ID = b_ID;
        this.STATUS = STATUS;
    }

    public int getB_ID() {
        return B_ID;
    }

    public void setB_ID(int b_ID) {
        B_ID = b_ID;
    }

    public String getSTATUS() {
        return STATUS;
    }

    public void setSTATUS(String STATUS) {
        this.STATUS = STATUS;
    }
}