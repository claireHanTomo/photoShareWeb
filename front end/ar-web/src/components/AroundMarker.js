import React from 'react';
import { Marker, InfoWindow } from 'react-google-maps';
import PropTypes from 'prop-types';
import blueMarkerUrl from '../assets/images/blue-marker.svg';

export class AroundMarker extends React.Component {
    static propTypes = {
        post: PropTypes.object.isRequired,
    }

    state = {
        isOpen: false,
    }

    handleToggle = () => {
        this.setState((prevState) => ({ isOpen: !prevState.isOpen }));
    }

    render() {
        const { user, message, url, location, type } = this.props.post;
        const { lat, lon } = location;
        const isImagePost = type === 'image';
        const customIcon = isImagePost ? undefined : {
            url: blueMarkerUrl,
            scaledSize: new window.google.maps.Size(26, 41),
        };
        return (
            <Marker
                position={{ lat, lng: lon }}
                onMouseOver={isImagePost ? this.handleToggle : undefined}
                onMouseOut={isImagePost ? this.handleToggle : undefined}
                onClick={isImagePost ? undefined: this.handleToggle}
                icon={customIcon}
            >
                {this.state.isOpen ? (
                    <InfoWindow>
                        <div>
                            {isImagePost
                                ? <img src={url} alt={message} className="around-marker-image"/>
                                : <video src={url} controls className="around-marker-video"/>}
                            <p>{`${user}: ${message}`}</p>
                        </div>
                    </InfoWindow>
                ) : null}
            </Marker>
        );
    }
}
