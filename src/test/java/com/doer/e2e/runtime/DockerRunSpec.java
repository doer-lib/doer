package com.doer.e2e.runtime;

import java.util.List;

/**
 * Arguments of {@code docker run} that differ between runtimes; name, network, port and {@code E2E_*} variables are
 * added by {@link com.doer.e2e.E2eEnvironment}.
 *
 * @param options mounts of the built artifact and other options
 * @param image   image of the runtime, or a JRE image
 * @param command command in the container; empty for the command of the image
 */
public record DockerRunSpec(List<String> options, String image, List<String> command) {
}
