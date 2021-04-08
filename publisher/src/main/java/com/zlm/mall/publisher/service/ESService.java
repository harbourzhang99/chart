package com.zlm.mall.publisher.service;

import java.util.Map;

/**
 * @author Harbour
 * @date 2021-04-08 9:18
 */
public interface ESService {

    Long getDauTotal (String date);

    Long getNewUserTotal(String date);

    Map<String, Long> getDauHour (String date);
}
