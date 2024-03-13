package ru.seuslab.vkuploader;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown = true)
public class VK_UserInfo {
    public long id;
    public String about;  
    public String first_name;  
    public String last_name;  
    public String bdate;
    public VK_City city;
    public VK_Contacts contacts;
    public VK_Personal personal;
    public String home_phone;
    public int has_mobile;
    public boolean can_access_closed;
    public boolean is_closed;
}

