package com.lubao.flink.day02;

public class OrderBean {

    public String province;

    public String city;

    public Double money;

    public OrderBean() {
    }

    public OrderBean(String province, String city, Double money) {
        this.province = province;
        this.city = city;
        this.money = money;
    }


    public static OrderBean of(String province, String city, Double money) {
        return new OrderBean(province, city, money);
    }
    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }
}
