package com.web;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.HashMap;
import java.util.Map;

@Controller
public class SampleController {
    @RequestMapping("/hello")
    public String getListaUtentiView(ModelMap map){
        map.put("name","helloWorld");
        return "home";
    }
    @RequestMapping("/hellojsp")
    public ModelAndView hellojsp(){
        // 页面位置 /WEB-INF/jsp/page/page.jsp
        ModelAndView mav = new ModelAndView("home");
        mav.addObject("name", "helloWorld");
        return mav;
    }

}
