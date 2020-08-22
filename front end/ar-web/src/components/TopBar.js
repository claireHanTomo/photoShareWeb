import React from 'react';
import logo from '../assets/images/logo.svg';
import { Icon} from '@iconify/react';
import logoutOutlined from "@ant-design/icons/lib/icons/LogoutOutlined"

export class TopBar extends React.Component {
    render() {
        return (
            <header className="App-header">
                <img src={logo} className="App-logo" alt="logo"/>
                <span className="App-title">Around</span>
                {this.props.isLoggedIn ?
                    <a className="logout" onClick={this.props.handleLogout}>
                        <Icon icon={logoutOutlined} />{' '} Logout </a> : null}
            </header>
        );
    }
}