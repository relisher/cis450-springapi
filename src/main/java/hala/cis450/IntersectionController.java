/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hala.cis450;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.ErrorController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author arelin
 */
@RestController
public class IntersectionController implements ErrorController {
    
    @Autowired
    private DynamoDBQuery dynamoQuery;
    
    @RequestMapping("/api/test")
    public String test() {
        return dynamoQuery.query();
    }
    
    
    @RequestMapping("/error")
    public String error() {
            return "<!DOCTYPE html>\n" +
            "<html>\n" +
            "<body>\n" +
            "\n" +
            "<marquee>You've reached the error route! Whoops </marquee>\n" +
            "</body>\n" +
            "</html>";
    }
    
    @Override
    public String getErrorPath() {
        return "/api/error";
    }
    
}
