/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hala.cis450;

import java.sql.SQLException;
import org.springframework.boot.autoconfigure.web.ErrorController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author arelin
 */
@RestController
public class IntersectionController implements ErrorController {
    
    DynamoDBQuery dynamoQuery = new DynamoDBQuery();
    
    @RequestMapping("/api/test")
    public String test() throws SQLException {
        return dynamoQuery.query();
    }
    
    @RequestMapping("/api/gender")
    public String gender() throws SQLException {
        return dynamoQuery.genderQuery();
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
