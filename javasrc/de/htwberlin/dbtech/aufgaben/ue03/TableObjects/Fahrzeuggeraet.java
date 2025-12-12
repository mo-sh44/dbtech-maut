package de.htwberlin.dbtech.aufgaben.ue03.TableObjects;

import java.sql.Date;

public class Fahrzeuggeraet {
    private long FZG_ID;
    private int FZ_ID;
    private String STATUS;
    private String TYP;
    private Date EINBAUDATUM;
    private Date AUSBAUDATUM;

    public Fahrzeuggeraet(long FZG_ID, int FZ_ID, String STATUS, String TYP, Date EINBAUDATUM, Date AUSBAUDATUM) {
        this.FZG_ID = FZG_ID;
        this.FZ_ID = FZ_ID;
        this.STATUS = STATUS;
        this.TYP = TYP;
        this.EINBAUDATUM = EINBAUDATUM;
        this.AUSBAUDATUM = AUSBAUDATUM;
    }

    public long getFZG_ID() {
        return FZG_ID;
    }

    public void setFZG_ID(long FZG_ID) {
        this.FZG_ID = FZG_ID;
    }

    public int getFZ_ID() {
        return FZ_ID;
    }

    public void setFZ_ID(int FZ_ID) {
        this.FZ_ID = FZ_ID;
    }

    public String getSTATUS() {
        return STATUS;
    }

    public void setSTATUS(String STATUS) {
        this.STATUS = STATUS;
    }

    public String getTYP() {
        return TYP;
    }

    public void setTYP(String TYP) {
        this.TYP = TYP;
    }

    public Date getEINBAUDATUM() {
        return EINBAUDATUM;
    }

    public void setEINBAUDATUM(Date EINBAUDATUM) {
        this.EINBAUDATUM = EINBAUDATUM;
    }

    public Date getAUSBAUDATUM() {
        return AUSBAUDATUM;
    }

    public void setAUSBAUDATUM(Date AUSBAUDATUM) {
        this.AUSBAUDATUM = AUSBAUDATUM;
    }
}