package vn.admicro;

import org.apache.commons.lang.builder.ToStringBuilder;
import scala.Serializable;


public class DataModel implements Serializable {
    String timeCreate;
    String cookieCreate;
    int browserCode;
    String browserVer;
    int osCode;
    String osVer;
    long ip; //ip kiểu long
    int locId;
    String domain;
    int siteId;
    int cId;
    String path;
    String referer;
    long guid;
    String flashVersion;
    String jre;
    String sr;
    String sc;
    int geographic;
    //public static final long serialVersionUID = -2849566368576647060L;

    public DataModel() {
    }

    public DataModel(String[] model){
        //String model[] = data.split("\t");
        timeCreate = model[0];
        cookieCreate = model[1];
        browserCode = Integer.parseInt(model[2]);
        browserVer = model[3];
        osCode = Integer.parseInt(model[4]);
        osVer = model[5];
        ip = Long.parseLong(model[6]);
        locId = Integer.parseInt(model[7]);
        domain = model[8];
        siteId = Integer.parseInt(model[9]);
        cId = Integer.parseInt(model[10]);
        path = model[11];
        referer = model[12];
        guid = Long.parseLong(model[13]);
        flashVersion = model[14];
        jre = model[15];
        sr = model[16];
        sc = model[17];
        geographic = Integer.parseInt(model[18]);
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this);
    }

    public String getTimeCreate() {
        return timeCreate;
    }

    public String getCookieCreate() {
        return cookieCreate;
    }

    public int getBrowserCode() {
        return browserCode;
    }

    public String getBrowserVer() {
        return browserVer;
    }

    public int getOsCode() {
        return osCode;
    }

    public String getOsVer() {
        return osVer;
    }

    public long getIp() {
        return ip;
    }

    public int getLocId() {
        return locId;
    }

    public String getDomain() {
        return domain;
    }

    public int getSiteId() {
        return siteId;
    }

    public int getcId() {
        return cId;
    }

    public String getPath() {
        return path;
    }

    public String getReferer() {
        return referer;
    }

    public long getGuid() {
        return guid;
    }

    public String getFlashVersion() {
        return flashVersion;
    }

    public String getJre() {
        return jre;
    }

    public String getSr() {
        return sr;
    }

    public String getSc() {
        return sc;
    }

    public int getGeographic() {
        return geographic;
    }
}
