package com.zlm.mall.publisher.comtroller;

import com.fasterxml.jackson.datatype.jsr310.ser.YearSerializer;
import com.zlm.mall.publisher.service.ESService;
import com.zlm.mall.publisher.service.impl.ESServiceImpl;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Harbour
 * @date 2021-04-08 9:53
 */
@RestController
public class PublisherController {

    @Resource
    ESService service = new ESServiceImpl();

    /*
    访问接口 ： http://localhost:8070/realtime-total?date=2020-08-18
    数据格式 ： [{"name":"xx", "id":"xxx", "value":xxx}, {"name":"xx", "id":"xxx", "value":xxx}]
    */
    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam(value = "date", defaultValue = "dau") String date) {
        ArrayList<HashMap<String, String>> totalList = new ArrayList<>(2);
        HashMap<String, String> dauMap = new HashMap<>();
        HashMap<String, String> userMap = new HashMap<>();

        dauMap.put("name", "新增日活");
        dauMap.put("id", "dau");
        dauMap.put("value", service.getDauTotal(date).toString());

        userMap.put("name", "新增用户");
        userMap.put("id", "user");
        userMap.put("value", service.getNewUserTotal(date).toString());

        totalList.add(dauMap);
        totalList.add(userMap);

        return totalList;
    }

    /*
    访问接口 ： http://localhost:8070/realtime-hour?date=2020-08-18
    数据格式 ： [ yesterday: {"name":"xx", "id":"xxx", "value":xxx},
                  today: {"name":"xx", "id":"xxx", "value":xxx}]
    */
    @RequestMapping("/realtime-hour")
    public Object realtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        Map<String, Map<String, Long>> dauHourMap = new HashMap<>();
        dauHourMap.put("yesterday", service.getDauHour(getYesterday(date)));
        dauHourMap.put("today", service.getDauHour(date));
        return dauHourMap;
    }

    /**
     * 将今日日期转为昨日
     * @param date 今天日期
     * @return 昨天日期
     */
    private String getYesterday(String date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date yesterday = null;
        try {
            Date today = simpleDateFormat.parse(date);
            yesterday = DateUtils.addDays(today, -1);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期转换失败");
        }
        return simpleDateFormat.format(yesterday);
    }
}
