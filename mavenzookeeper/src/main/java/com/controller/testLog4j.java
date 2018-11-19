package com.controller;


import com.utils.RedisCacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/testLog4j")
public class testLog4j {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @RequestMapping("/testLog4j")
    @ResponseBody
    public String testLog4j(){
        try{
            logger.info("this is my test");
            return "ok";
        }catch (Exception e){
            e.printStackTrace();
            return "error";
        }

    }
}
