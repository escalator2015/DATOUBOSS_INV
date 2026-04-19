idf_component_register(SRCS "main.cpp"
                       PRIV_REQUIRES nvs_flash esp_timer esp_wifi esp_event esp_netif
                                    esp_http_server esp_http_client mbedtls json
                                    spiffs esp_driver_uart esp_driver_gpio
                       EMBED_TXTFILES "inv_ui.html"
                       INCLUDE_DIRS "")
