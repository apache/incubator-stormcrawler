package com.digitalpebble.storm.fetchqueue;

public class Message {

	protected String content;
	protected String id;
	
	public Message (String content, String id){
		this.content = content;
		this.id = id;
	}
	
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	

}
