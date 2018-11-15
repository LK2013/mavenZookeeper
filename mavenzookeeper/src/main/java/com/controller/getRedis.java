package com.controller;

import com.utils.RedisCacheUtil;
import com.utils.ZkClientService;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/redisController")
public class getRedis {
    @RequestMapping("/getRedis")
    @ResponseBody
    public String getRedis(){
       try{
           RedisCacheUtil redisCacheUtil=new RedisCacheUtil();
           redisCacheUtil.getJedisCluster();
           Object country = RedisCacheUtil.getRedis("country");
           System.out.println(country.toString());
           return "ok";
        }catch (Exception e){
            e.printStackTrace();
            return "error";
        }

    }
}
