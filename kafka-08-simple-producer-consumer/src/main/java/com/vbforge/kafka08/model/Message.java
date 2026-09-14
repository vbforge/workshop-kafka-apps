package com.vbforge.kafka08.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Message {

    private String id;
    private String content;
    private LocalDateTime timestamp;

}
