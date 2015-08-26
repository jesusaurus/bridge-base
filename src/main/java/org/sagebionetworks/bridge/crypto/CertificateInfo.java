package org.sagebionetworks.bridge.crypto;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Captures the minimum information asked by the openssl interactive interface
 * when creating a self-signed certificate.
 * <p>
 * Example:
 * <p>
 * <li>Country Name (2 letter code): US
 * <li>State or Province Name (full name): Washington
 * <li>Locality Name (eg, city): Seattle
 * <li>Organization Name (eg, company): Sage Bionetworks
 * <li>Organizational Unit Name (eg, section): Platform
 * <li>Common Name (e.g. server FQDN or YOUR name): www.sagebridge.org
 * <li>Email Address: bridge@sagebridge.org
 */
public class CertificateInfo {

    private final String country;
    private final String state;
    private final String city;
    private final String organization;
    private final String team;
    private final String fqdn;
    private final String email;

    private CertificateInfo(String country, String state, String city,
            String organization, String team, String fqdn, String email) {
        this.country = country;
        this.state = state;
        this.city = city;
        this.organization = organization;
        this.team = team;
        this.fqdn = fqdn;
        this.email = email;
    }

    public String getCountry() {
        return country;
    }

    public String getState() {
        return state;
    }

    public String getCity() {
        return city;
    }

    public String getOrganization() {
        return organization;
    }

    public String getTeam() {
        return team;
    }

    public String getFqdn() {
        return fqdn;
    }

    public String getEmail() {
        return email;
    }

    public static class Builder {

        private String country = "US";
        private String state = "WA";
        private String city = "Seattle";
        private String organization = "Sage Bionetworks";
        private String team = organization;
        private String fqdn = organization;
        private String email = "";

        /**
         * Country name. Two-letter code, e.g., US.
         */
        public Builder country(String country) {
            checkNotNull(country);
            this.country = country;
            return this;
        }

        /**
         * State or province name. Full name, e.g., Washington.
         */
        public Builder state(String state) {
            checkNotNull(state);
            this.state = state;
            return this;
        }

        /**
         * City name, e.g., Seattle.
         */
        public Builder city(String city) {
            checkNotNull(city);
            this.city = city;
            return this;
        }

        /**
         * Organization name, e.g., Sage Bionetworks.
         */
        public Builder organization(String org) {
            checkNotNull(org);
            this.organization = org;
            return this;
        }

        /**
         * Team name, e.g., Bridge.
         */
        public Builder team(String team) {
            checkNotNull(team);
            this.team = team;
            return this;
        }

        /**
         * FQDN (or your name), e.g., www.sagebridge.org.
         */
        public Builder fqdn(String fqdn) {
            checkNotNull(fqdn);
            this.fqdn = fqdn;
            return this;
        }

        /**
         * Email address.
         */
        public Builder email(String email) {
            checkNotNull(email);
            this.email = email;
            return this;
        }

        public CertificateInfo build() {
            return new CertificateInfo(country, state, city, organization,
                    team, fqdn, email);
        }
    }
}
