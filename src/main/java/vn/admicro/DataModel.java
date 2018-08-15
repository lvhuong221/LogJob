package vn.admicro;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DataModel {
    DateTime timeCreate;
    DateTime cookieCreate;
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

    public DataModel() {
    }

    public DataModel(String data){
        String model[] = data.split("\t");
        setTimeCreate(model[0]);
        setCookieCreate(model[1]);
        setBrowserCode(Integer.parseInt(model[2]));
        setBrowserVer(model[3]);
        setOsCode(Integer.parseInt(model[4]));
        setOsVer(model[5]);
        setIp(Long.parseLong(model[6]));
        setLocId(Integer.parseInt(model[7]));
        setDomain(model[8]);
        setSiteId(Integer.parseInt(model[9]));
        setcId(Integer.parseInt(model[10]));
        setPath(model[11]);
        setReferer(model[12]);
        setGuid(Long.parseLong(model[13]));
        setFlashVersion(model[14]);
        setJre(model[15]);
        setSr(model[16]);
        setSc(model[17]);
        setGeographic(Integer.parseInt(model[18]));
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this);
    }

    public String getTimeCreate() {
        return timeCreate.toString();
    }

    public void setTimeCreate(String timeCreate) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        this.timeCreate = formatter.parseDateTime(timeCreate);
    }

    public String getCookieCreate() {
        return cookieCreate.toString();
    }

    public void setCookieCreate(String cookieCreate) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        this.cookieCreate = formatter.parseDateTime(cookieCreate);
    }

    public int getBrowserCode() {
        return browserCode;
    }

    public void setBrowserCode(int browserCode) {
        this.browserCode = browserCode;
    }

    public String getBrowserVer() {
        return browserVer;
    }

    public void setBrowserVer(String browserVer) {
        this.browserVer = browserVer;
    }

    public int getOsCode() {
        return osCode;
    }

    public void setOsCode(int osCode) {
        this.osCode = osCode;
    }

    public String getOsVer() {
        return osVer;
    }

    public void setOsVer(String osVer) {
        this.osVer = osVer;
    }

    public long getIp() {
        return ip;
    }

    public void setIp(long ip) {
        this.ip = ip;
    }

    public int getLocId() {
        return locId;
    }

    public void setLocId(int locId) {
        this.locId = locId;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }

    public int getcId() {
        return cId;
    }

    public void setcId(int cId) {
        this.cId = cId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public long getGuid() {
        return guid;
    }

    public void setGuid(long guid) {
        this.guid = guid;
    }

    public String getFlashVersion() {
        return flashVersion;
    }

    public void setFlashVersion(String flashVersion) {
        this.flashVersion = flashVersion;
    }

    public String getJre() {
        return jre;
    }

    public void setJre(String jre) {
        this.jre = jre;
    }

    public String getSr() {
        return sr;
    }

    public void setSr(String sr) {
        this.sr = sr;
    }

    public String getSc() {
        return sc;
    }

    public void setSc(String sc) {
        this.sc = sc;
    }

    public int getGeographic() {
        return geographic;
    }

    public void setGeographic(int geographic) {
        this.geographic = geographic;
    }
}
