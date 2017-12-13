/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hala.cis450;

import org.springframework.boot.autoconfigure.web.ErrorController;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author arelin
 */
@RestController
public class IntersectionController implements ErrorController {
    
    DynamoDBQuery dynamoQuery = new DynamoDBQuery();
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/test")
    public String test(){
        return dynamoQuery.query();
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/gender-married")
    public String genderMarried() {
        return dynamoQuery.marriedGender();
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/gender-orientation")
    public String genderOrientation() {
        return dynamoQuery.genderOrientation();
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/married")
    public String married() {
        return dynamoQuery.married();
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/married-location")
    public String marriedLocation() {
        return dynamoQuery.marriedLocation();
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/young-location")
    public String youthful() {
        return dynamoQuery.youngUsers();
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/lgbtq-demographic")
    public String lgbtq() {
        return dynamoQuery.lgbtqLocation();
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/dynamicOne")
    public String age(@RequestParam(value = "age") int age,
            @RequestParam(value = "city") String city) {
        return dynamoQuery.dage(city, age);
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/dynamicTwo")
    public String income(@RequestParam(value = "income") int income,
            @RequestParam(value = "city") String city) {
        
        return dynamoQuery.dincome(city, income);
    }
    
    @CrossOrigin(origins = "*")
    @RequestMapping("/api/dynamicThree")
    public String income(@RequestParam(value = "height") int height) {
        
        return dynamoQuery.dheightlocation(height);
    }
    
    @CrossOrigin(origins = "*")
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
