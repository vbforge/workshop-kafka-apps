package com.vbforge.case03.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MyMessageObject {

    private String id;
    private String content;
    private LocalDateTime timestamp;

}
