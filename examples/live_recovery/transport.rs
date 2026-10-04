//! Preserve a meaningful EOF error when attached to tokio-modbus 0.17.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// tokio-modbus 0.17 uses last_os_error() when its response stream ends.
// Convert an observed read EOF into an explicit transport error instead of
// allowing an unrelated thread-local errno to determine retry classification.
#[derive(Debug)]
pub(super) struct ModbusTransport<T>(pub(super) T);
impl<T: AsyncRead + Unpin> AsyncRead for ModbusTransport<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled = buf.filled().len();
        let remaining = buf.remaining();
        match Pin::new(&mut self.0).poll_read(cx, buf) {
            Poll::Ready(Ok(())) if remaining > 0 && buf.filled().len() == filled => {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Modbus peer closed before a response",
                )))
            }
            result => result,
        }
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for ModbusTransport<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_modbus::prelude::*;

    #[tokio::test]
    async fn closed_peer_is_a_retryable_modbus_transport_error() {
        let (client, mut peer) = tokio::io::duplex(64);
        let mut context = tcp::attach(ModbusTransport(client));
        let request = context.read_holding_registers(1, 3);
        let close = async move {
            let mut frame = [0; 12];
            peer.read_exact(&mut frame).await.unwrap();
            // Close after reading the request, without sending a response.
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(request, close)
        })
        .await
        .unwrap();
        let tokio_modbus::Error::Transport(error) = result.unwrap_err() else {
            panic!("peer closure was not classified as a transport error");
        };
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(super::super::retryable_read_error(&error));
        assert!(!super::super::retryable_read_error(&io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid response"
        )));
    }
}
