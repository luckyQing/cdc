package io.github.collin.cdc.common.dto.cache;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    private String applicationId;

    private String jobId;

}