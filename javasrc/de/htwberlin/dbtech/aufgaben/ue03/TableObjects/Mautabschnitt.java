package de.htwberlin.dbtech.aufgaben.ue03.TableObjects;

public class Mautabschnitt {
    private int ABSCHNITTS_ID;
    private int LAENGE;
    private String START_KOORDINATE;
    private String ZIEL_KOORDINATE;
    private String NAME;
    private String ABSCHNITTSTYP;

    public Mautabschnitt(int ABSCHNITTS_ID, int LAENGE, String START_KOORDINATE, String ZIEL_KOORDINATE, String NAME, String ABSCHNITTSTYP) {
        this.ABSCHNITTS_ID = ABSCHNITTS_ID;
        this.LAENGE = LAENGE;
        this.START_KOORDINATE = START_KOORDINATE;
        this.ZIEL_KOORDINATE = ZIEL_KOORDINATE;
        this.NAME = NAME;
        this.ABSCHNITTSTYP = ABSCHNITTSTYP;
    }

    public int getABSCHNITTS_ID() {
        return ABSCHNITTS_ID;
    }

    public void setABSCHNITTS_ID(int ABSCHNITTS_ID) {
        this.ABSCHNITTS_ID = ABSCHNITTS_ID;
    }

    public int getLAENGE() {return LAENGE;}

    public void setLAENGE(int LAENGE) {
        this.LAENGE = LAENGE;
    }

    public String getSTART_KOORDINATE() {
        return START_KOORDINATE;
    }

    public void setSTART_KOORDINATE(String START_KOORDINATE) {
        this.START_KOORDINATE = START_KOORDINATE;
    }

    public String getZIEL_KOORDINATE() {
        return ZIEL_KOORDINATE;
    }

    public void setZIEL_KOORDINATE(String ZIEL_KOORDINATE) {
        this.ZIEL_KOORDINATE = ZIEL_KOORDINATE;
    }

    public String getNAME() {
        return NAME;
    }

    public void setNAME(String NAME) {
        this.NAME = NAME;
    }

    public String getABSCHNITTSTYP() {
        return ABSCHNITTSTYP;
    }

    public void setABSCHNITTSTYP(String ABSCHNITTSTYP) {
        this.ABSCHNITTSTYP = ABSCHNITTSTYP;
    }
}